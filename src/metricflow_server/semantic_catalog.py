"""Native MetricFlow catalog with full Entity links and resolved time grains.

Adapted for the pinned 0.214 engine API; no CLI or adapter initialization here.
"""
import datetime
import decimal
import math
import time

def serialize(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def catalog(engine, manifest, metrics, subjects):
    started=loaded=time.perf_counter()
    payload={"metrics":metrics,"subjects":subjects}
    names = payload['metrics']
    subject_map={s['id']:s for s in payload.get('subjects',[])}
    entity_candidates={}
    for model in manifest.get('semantic_models',[]):
        subject=subject_map.get(model['name'])
        if not subject:continue
        for entity in model.get('entities',[]):
            if entity.get('type') in ('primary','unique'):
                entity_candidates.setdefault(entity['name'],[]).append({**subject,'field':entity.get('expr') or entity['name'],'entity_type':entity['type']})
    from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
    from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
    from metricflow_semantic_interfaces.references import MetricReference
    from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
    time_items=engine._semantic_manifest_lookup.metric_lookup.get_common_group_by_items(
        metric_references=tuple(MetricReference(element_name=n) for n in names),
        set_filter=GroupByItemSetFilter.create(any_properties_denylist={GroupByItemProperty.ENTITY,GroupByItemProperty.DATE_PART,GroupByItemProperty.LOCAL_LINKED,GroupByItemProperty.METRIC}))
    time_specs={}
    for item in time_items.annotated_specs:
        if item.element_type is LinkableElementType.TIME_DIMENSION:
            spec=item.spec
            base='__'.join([e.element_name for e in spec.entity_links]+[spec.element_name])
            time_specs.setdefault(base,[]).append((spec.dunder_name,spec.time_granularity.name))
    dims = engine.list_dimensions(metric_names=names)
    dimensions = []
    for d in dims:
        origin=d.semantic_model_reference.semantic_model_name if d.semantic_model_reference else None
        path=[e.element_name for e in d.entity_links]
        meta=dict(origin=subject_map.get(origin),path=path)
        if 'TIME' in str(d.type):
            dimensions.extend(dict(name=name,label=d.label or d.description or d.name,type='time',base=d.granularity_free_dunder_name,grain=grain,**meta) for name,grain in sorted(set(time_specs.get(d.granularity_free_dunder_name,[]))))
        else:
            dimensions.append(dict(name=d.dunder_name,label=d.label or d.description or d.dunder_name,type='categorical',**meta))
    # 0.214's public entities_for_metrics() drops entity-link prefixes. Read the
    # same native resolved set it uses, retaining each full spec.dunder_name.
    resolved=engine._semantic_manifest_lookup.metric_lookup.get_common_group_by_items(
        metric_references=tuple(MetricReference(element_name=n) for n in names),
        set_filter=GroupByItemSetFilter.create(any_properties_allowlist={GroupByItemProperty.ENTITY}))
    entities=[];seen=set()
    for annotated in resolved.annotated_specs:
        spec=annotated.spec
        if spec.dunder_name in seen:continue
        seen.add(spec.dunder_name)
        entities.append(dict(name=spec.dunder_name,label=spec.element_name,type='entity',entity=spec.element_name,
                             path=[e.element_name for e in spec.entity_links],candidates=entity_candidates.get(spec.element_name,[])))
    entities.sort(key=lambda e:e['name'])
    dimensions.extend(entities)
    result = dict(dimensions=dimensions,entities=entities,engine='MetricFlow', metrics=names,
                  timings_ms=dict(engine=round((loaded-started)*1000,2),dimensions=round((time.perf_counter()-loaded)*1000,2)))
    return result


def execute_query(engine,payload):
    from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
    names=payload["metrics"]
    result={}
    import datetime as dt
    request = MetricFlowQueryRequest.create(
        metric_names=names, group_by_names=payload.get('group_by', []),
        order_by_names=payload.get('order_by') or None,
        where_constraints=[payload['where']] if payload.get('where') else None,
        time_constraint_start=dt.datetime.fromisoformat(payload['start']) if payload.get('start') else None,
        time_constraint_end=dt.datetime.fromisoformat(payload['end']) if payload.get('end') else None,
        limit=payload.get('limit', 100))
    if payload['action'] == 'compile':
        explained = engine.explain(request)
        result['sql'] = explained.sql_statement.sql
    else:
        queried = engine.query(request)
        table = queried.result_df
        result.update(sql=queried.sql, columns=list(table.column_names),
                      rows=[{name: serialize(value) for name, value in zip(table.column_names, row)} for row in table.rows])
    return result
