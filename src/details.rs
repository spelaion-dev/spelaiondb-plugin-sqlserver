//! A relation's properties, partitions and dependencies (SpelaionDB's SDB-393), from SQL
//! Server's catalog views.
//!
//! Dependencies are what SQL Server records: `sys.sql_expression_dependencies` (the objects a
//! view, function, procedure or trigger names in its text, recorded when it is created), foreign
//! keys both ways, and the triggers on a table. What dynamic SQL touches is never recorded.
//!
//! A partitioned table's partitions have numbers, not names: each is listed by its number,
//! with the range of values its partition function gives it.

use spelaion_driver_api::{
    Dependencies, DependencyInfo, DependencyKind, DriverError, ObjectProperty, PartitionInfo,
    Partitioning, Result, property,
};

use crate::SqlServerConnection;

fn text(row: &[Option<String>], i: usize) -> String {
    row.get(i).cloned().flatten().unwrap_or_default()
}

fn opt(row: &[Option<String>], i: usize) -> Option<String> {
    row.get(i).cloned().flatten().filter(|s| !s.is_empty())
}

fn flag(row: &[Option<String>], i: usize) -> bool {
    row.get(i)
        .cloned()
        .flatten()
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

fn missing(schema: &str, name: &str) -> DriverError {
    DriverError::Other(format!("there is no table or view {schema}.{name}"))
}

/// The relation's object id, from `@P1` (schema) and `@P2` (name).
const OBJECT: &str = "object_id(quotename(@P1) + '.' + quotename(@P2))";

pub async fn properties(
    conn: &SqlServerConnection,
    schema: &str,
    name: &str,
) -> Result<Vec<ObjectProperty>> {
    let rows = conn
        .rows(
            &format!(
                "select coalesce(user_name(o.principal_id), user_name(s.principal_id)),
                        convert(varchar(23), o.create_date, 121),
                        convert(varchar(23), o.modify_date, 121),
                        ds.name, ds.type_desc,
                        (select top 1 p.data_compression_desc from sys.partitions p
                         where p.object_id = o.object_id and p.index_id in (0, 1)),
                        t.temporal_type_desc,
                        cast(t.is_memory_optimized as int)
                 from sys.objects o
                 join sys.schemas s on s.schema_id = o.schema_id
                 left join sys.tables t on t.object_id = o.object_id
                 left join sys.indexes i on i.object_id = o.object_id and i.index_id in (0, 1)
                 left join sys.data_spaces ds on ds.data_space_id = i.data_space_id
                 where o.object_id = {OBJECT} and o.type in ('U', 'V')"
            ),
            vec![schema.to_string(), name.to_string()],
        )
        .await?;
    let row = rows.first().ok_or_else(|| missing(schema, name))?;
    let mut out = Vec::new();
    let mut push = |name: &str, value: Option<String>| {
        if let Some(value) = value {
            out.push(ObjectProperty {
                name: name.into(),
                value,
            });
        }
    };
    push(property::OWNER, opt(row, 0));
    push(property::CREATED, opt(row, 1));
    push(property::UPDATED, opt(row, 2));
    match opt(row, 4).as_deref() {
        Some("PARTITION_SCHEME") => push("partition_scheme", opt(row, 3)),
        Some(_) => push("filegroup", opt(row, 3)),
        None => {}
    }
    push("compression", opt(row, 5).filter(|c| c != "NONE"));
    push(
        "temporal_type",
        opt(row, 6).filter(|t| t != "NON_TEMPORAL_TABLE"),
    );
    if flag(row, 7) {
        push("memory_optimized", Some("yes".into()));
    }
    Ok(out)
}

/// A boundary value as SQL: numbers as they are, anything else quoted.
fn literal(value: &str, numeric: bool) -> String {
    if numeric {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "''"))
    }
}

/// The range each partition of a `RANGE LEFT` (`right` false) or `RANGE RIGHT` function holds,
/// from its boundary values in order: one more partition than boundaries.
fn bounds(boundaries: &[String], right: bool) -> Vec<String> {
    let (low, high) = if right { (">=", "<") } else { (">", "<=") };
    let mut out = Vec::new();
    for i in 0..=boundaries.len() {
        let bound = match (i.checked_sub(1).map(|j| &boundaries[j]), boundaries.get(i)) {
            (None, Some(upper)) => format!("{high} {upper}"),
            (Some(lower), Some(upper)) => format!("{low} {lower} and {high} {upper}"),
            (Some(lower), None) => format!("{low} {lower}"),
            (None, None) => String::new(),
        };
        out.push(bound);
    }
    out
}

pub async fn partitions(
    conn: &SqlServerConnection,
    schema: &str,
    name: &str,
) -> Result<Option<Partitioning>> {
    let params = || vec![schema.to_string(), name.to_string()];
    let scheme = conn
        .rows(
            &format!(
                "select pf.function_id, cast(pf.boundary_value_on_right as int), c.name
                 from sys.indexes i
                 join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
                 join sys.partition_functions pf on pf.function_id = ps.function_id
                 join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id
                                          and ic.partition_ordinal > 0
                 join sys.columns c on c.object_id = ic.object_id and c.column_id = ic.column_id
                 where i.object_id = {OBJECT} and i.index_id in (0, 1)
                 order by ic.partition_ordinal"
            ),
            params(),
        )
        .await?;
    let Some(first) = scheme.first() else {
        return Ok(None);
    };
    let function = text(first, 0);
    let right = flag(first, 1);
    let key = scheme
        .iter()
        .map(|row| text(row, 2))
        .collect::<Vec<_>>()
        .join(", ");
    let boundaries: Vec<String> = conn
        .rows(
            "select convert(nvarchar(4000), value, 121),
                    case when cast(sql_variant_property(value, 'BaseType') as nvarchar(128))
                              in ('tinyint', 'smallint', 'int', 'bigint', 'decimal', 'numeric',
                                  'float', 'real', 'money', 'smallmoney')
                         then 1 else 0 end
             from sys.partition_range_values
             where function_id = @P1 order by boundary_id",
            vec![function],
        )
        .await?
        .iter()
        .map(|row| literal(&text(row, 0), flag(row, 1)))
        .collect();
    let ranges = bounds(&boundaries, right);
    let counts = conn
        .rows(
            &format!(
                "select partition_number, rows from sys.partitions
                 where object_id = {OBJECT} and index_id in (0, 1) order by partition_number"
            ),
            params(),
        )
        .await?;
    Ok(Some(Partitioning {
        method: if right { "range right" } else { "range left" }.into(),
        key,
        partitions: counts
            .iter()
            .map(|row| {
                let number: usize = text(row, 0).parse().unwrap_or(0);
                PartitionInfo {
                    schema: schema.to_string(),
                    name: number.to_string(),
                    bound: number
                        .checked_sub(1)
                        .and_then(|i| ranges.get(i))
                        .filter(|b| !b.is_empty())
                        .cloned(),
                    estimated_rows: opt(row, 1).and_then(|n| n.parse().ok()),
                    subpartitioned: false,
                }
            })
            .collect(),
    }))
}

fn object_kind(kind: &str) -> DependencyKind {
    match kind.trim() {
        "U" => DependencyKind::Table,
        "V" => DependencyKind::View,
        "P" | "PC" => DependencyKind::Procedure,
        "FN" | "IF" | "TF" | "FS" | "FT" => DependencyKind::Function,
        "TR" => DependencyKind::Trigger,
        "SO" => DependencyKind::Sequence,
        "TT" => DependencyKind::Type,
        _ => DependencyKind::Other,
    }
}

fn kind_order(kind: DependencyKind) -> u8 {
    match kind {
        DependencyKind::Table => 0,
        DependencyKind::View => 1,
        DependencyKind::MaterializedView => 2,
        DependencyKind::ForeignKey => 3,
        DependencyKind::Trigger => 4,
        DependencyKind::Function => 5,
        DependencyKind::Procedure => 6,
        DependencyKind::Sequence => 7,
        DependencyKind::Type => 8,
        DependencyKind::Index => 9,
        DependencyKind::Other => 10,
    }
}

fn sorted(mut list: Vec<DependencyInfo>) -> Vec<DependencyInfo> {
    list.sort_by(|a, b| {
        (kind_order(a.kind), &a.schema, &a.name, &a.detail).cmp(&(
            kind_order(b.kind),
            &b.schema,
            &b.name,
            &b.detail,
        ))
    });
    list.dedup();
    list
}

/// Rows of (schema, name, `sys.objects` type, detail).
fn infos(rows: &[Vec<Option<String>>], kind: Option<DependencyKind>) -> Vec<DependencyInfo> {
    rows.iter()
        .map(|row| DependencyInfo {
            schema: text(row, 0),
            name: text(row, 1),
            kind: kind.unwrap_or_else(|| object_kind(&text(row, 2))),
            detail: opt(row, 3),
        })
        .collect()
}

/// A foreign key being read: the table on the side shown, its name, and its columns on each side.
type Key = (String, String, String, Vec<String>, Vec<String>);

/// A foreign key's columns, grouped here: `STRING_AGG` is only in 2017 and later.
fn keys(rows: &[Vec<Option<String>>], kind: DependencyKind) -> Vec<DependencyInfo> {
    let mut out: Vec<Key> = Vec::new();
    for row in rows {
        let (schema, table, key) = (text(row, 0), text(row, 1), text(row, 2));
        match out.last_mut() {
            Some(last) if last.0 == schema && last.1 == table && last.2 == key => {
                last.3.push(text(row, 3));
                last.4.push(text(row, 4));
            }
            _ => out.push((schema, table, key, vec![text(row, 3)], vec![text(row, 4)])),
        }
    }
    out.into_iter()
        .map(|(schema, name, key, from, to)| DependencyInfo {
            schema,
            name,
            kind,
            detail: Some(format!("{key} ({}) → ({})", from.join(", "), to.join(", "))),
        })
        .collect()
}

pub async fn dependencies(
    conn: &SqlServerConnection,
    schema: &str,
    name: &str,
) -> Result<Option<Dependencies>> {
    let params = || vec![schema.to_string(), name.to_string()];
    let found = conn
        .rows(
            &format!("select 1 from sys.objects where object_id = {OBJECT} and type in ('U', 'V')"),
            params(),
        )
        .await?;
    if found.is_empty() {
        return Err(missing(schema, name));
    }
    // What its text names (a view's), and what names it in theirs.
    let uses = conn
        .rows(
            &format!(
                "select distinct schema_name(o.schema_id), o.name, o.type, null
                 from sys.sql_expression_dependencies d
                 join sys.objects o on o.object_id = d.referenced_id
                 where d.referencing_id = {OBJECT} and d.referenced_id <> d.referencing_id"
            ),
            params(),
        )
        .await?;
    let used_by = conn
        .rows(
            &format!(
                "select distinct schema_name(o.schema_id), o.name, o.type,
                        case when o.type = 'TR' then 'on ' + object_name(o.parent_object_id) end
                 from sys.sql_expression_dependencies d
                 join sys.objects o on o.object_id = d.referencing_id
                 where d.referenced_id = {OBJECT} and d.referenced_id <> d.referencing_id"
            ),
            params(),
        )
        .await?;
    let triggers = conn
        .rows(
            &format!(
                "select schema_name(o.schema_id), t.name, 'TR', 'on ' + o.name
                 from sys.triggers t join sys.objects o on o.object_id = t.parent_id
                 where t.parent_id = {OBJECT}"
            ),
            params(),
        )
        .await?;
    let key_columns = "from sys.foreign_keys fk
         join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id
         join sys.columns pc on pc.object_id = fkc.parent_object_id and pc.column_id = fkc.parent_column_id
         join sys.columns rc on rc.object_id = fkc.referenced_object_id and rc.column_id = fkc.referenced_column_id";
    let outgoing = conn
        .rows(
            &format!(
                "select schema_name(rt.schema_id), rt.name, fk.name, pc.name, rc.name
                 {key_columns}
                 join sys.tables rt on rt.object_id = fkc.referenced_object_id
                 where fk.parent_object_id = {OBJECT}
                 order by fk.name, fkc.constraint_column_id"
            ),
            params(),
        )
        .await?;
    let incoming = conn
        .rows(
            &format!(
                "select schema_name(pt.schema_id), pt.name, fk.name, pc.name, rc.name
                 {key_columns}
                 join sys.tables pt on pt.object_id = fkc.parent_object_id
                 where fk.referenced_object_id = {OBJECT}
                 order by 1, 2, 3, fkc.constraint_column_id"
            ),
            params(),
        )
        .await?;
    let mut uses = infos(&uses, None);
    uses.extend(keys(&outgoing, DependencyKind::Table));
    let mut used_by = infos(&used_by, None);
    used_by.extend(infos(&triggers, Some(DependencyKind::Trigger)));
    used_by.extend(keys(&incoming, DependencyKind::ForeignKey));
    Ok(Some(Dependencies {
        uses: sorted(uses),
        used_by: sorted(used_by),
        untracked: Vec::new(),
    }))
}

#[cfg(test)]
mod tests {
    use super::{bounds, literal};

    #[test]
    fn partition_ranges_follow_the_function_side() {
        let values = vec![literal("2025", true), literal("2026", true)];
        assert_eq!(
            bounds(&values, true),
            ["< 2025", ">= 2025 and < 2026", ">= 2026"]
        );
        assert_eq!(
            bounds(&values, false),
            ["<= 2025", "> 2025 and <= 2026", "> 2026"]
        );
        assert_eq!(literal("it's", false), "'it''s'");
    }
}
