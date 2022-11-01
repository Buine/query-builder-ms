const knex = require('knex')({ client: 'pg' });

const createQuery = function (body) {
    const queryBuilder = knex.queryBuilder()
    getTables(queryBuilder, body.query_json.tables, body.queries_from_integration_code)
    getFilters(queryBuilder, body.query_json.filters)
    groupByListColumns = getGroupBy(queryBuilder, body.query_json.group_by)
    getSelect(queryBuilder, body.query_json.columns, groupByListColumns)
    getOrders(queryBuilder, body.query_json.order)
    console.log(`Query generted: ${queryBuilder.toString()}`)
    return queryBuilder.toString()
}

const typeFilters = { "EQ": "=", "NOT_EQ": "!=", "NOT_ILIKE": "not ilike", "ILIKE": "ilike", "GREATER": ">", "GREATER_EQ": ">=", "LESS": "<", "LESS_EQ": "<=", "GROUP": null, "IS": knex.raw("is"), "IS_NOT": knex.raw("is not") }
const logicGatesFilter = {
    AND: (builder, cb, p1, p2, p3) => cb ? builder.andWhere(cb) : builder.andWhere(p1, p2, p3),
    OR: (builder, cb, p1, p2, p3)  => cb ? builder.orWhere(cb): builder.orWhere(p1, p2, p3)
}

const joinsType = {
    LEFT: (qb, join) => qb.leftJoin(join.rigthTable, join.columnLeft, join.columnRigth),
    INNER: (qb, join) => qb.innerJoin(join.rigthTable, join.columnLeft, join.columnRigth),
    RIGTH: (qb, join) => qb.rightJoin(join.rigthTable, join.columnLeft, join.columnRigth)
}

const typesParamFilter = {
    "TEXT": "::TEXT",
    "DATE": "::TIMESTAMP",
    "NUMBER": "::NUMERIC",
    "NONE": ""
}

const functionsParamFilter = {
    "NOW": "NOW()"
}

const selectTypeFunction = {
    COLUMN: (qb, column) => {
        var table_column = column.table_column
        var name = ""
        if (table_column.alias) {
            if (table_column.column_alias) {
                name = knex.raw(`"${table_column.alias}"."??" as "${table_column.column_alias}"`, [knex.raw(table_column.column_name)])
            } else {
                name += `${table_column.alias}.${table_column.column_name}`
            }
        } else {
            name += table_column.schema_name != null ? `${table_column.schema_name}.` : ""
            name += table_column.table_name != null ? `${table_column.table_name}.${table_column.column_name}` : table_column.column_name
        }
        console.log("Column name select ", name)
        qb.column(name)
    },
    GENERATED: (qb, column) => {
        var generated_column = column.generated_column
        generatedColumnFunctions[generated_column.function_name](qb, generated_column)
    }
}

const generatedColumnFunctions = {
    CONCAT: (qb, generated_column) => {
        var args = []
        for (let i = 0; i < generated_column.params.length; i++) {
            args.push(getParam(i, generated_column.params))
        }
        var column = knex.raw('CONCAT('+ args.map(it => {
            return typeof(it) == "string" ? '??' : '?' // Si es un parametro o una columna
        }).join(',') +') AS ??', [...args, generated_column.name])
        console.log(column)
        qb.column(column)
    },
    DATE_PART: (qb, generated_column) => {
        param = getParam(1, generated_column.params)
        qb.column(knex.raw('DATE_PART(?, TIMESTAMP '+(typeof(param) == "string" ? '??' : '?')+') AS ??', [getParam(0, generated_column.params), param, generated_column.name]))
    },
    COUNT: (qb, generated_column) => {qb.column(knex.raw('COUNT(??) AS "??"', [getParam(0, generated_column.params), knex.raw(generated_column.name)]))},
    MAX: (qb, generated_column) => {qb.column(knex.raw('MAX(??) AS "??"', [getParam(0, generated_column.params), knex.raw(generated_column.name)]))},
    MIN: (qb, generated_column) => {qb.column(knex.raw('MIN(??) AS "??"', [getParam(0, generated_column.params), knex.raw(generated_column.name)]))},
    SUM: (qb, generated_column) => {qb.column(knex.raw('SUM(??) AS "??"', [getParam(0, generated_column.params), knex.raw(generated_column.name)]))},
    AVG: (qb, generated_column) => {qb.column(knex.raw('AVG(??) AS "??"', [getParam(0, generated_column.params), knex.raw(generated_column.name)]))},
}

function getSelect(qb, columns, columnsGroupBy) {
    isGroupActive = false
    if (columns.length == 0) {
        return 
    } else if (columnsGroupBy.length > 0) {
        isGroupActive = true
    }

    columns.forEach(column => {
        selectTypeFunction[column.type](qb, column)
    })
}

function getGroupBy(qb, group_by) {
    groupByArray = []
    group_by.forEach(currentGroupBy => {
        var name = ""
        if (currentGroupBy.alias) {
            name += `${currentGroupBy.alias}.${currentGroupBy.column_name}`
        } else {
            name += currentGroupBy.schema_name != null ? `${currentGroupBy.schema_name}.` : ""
            name += currentGroupBy.table_name != null ? `${currentGroupBy.table_name}.${currentGroupBy.column_name}` : currentGroupBy.column_name
        }
        groupByArray.push(name)
        qb.groupBy(name)
    })
    return groupByArray 
}

function getFilters(qb, filters) {
    filters.forEach(filter => {
        console.log(filter)
        if (filter.type == "GROUP") {
            if (filter.gate_logic_previous == null) {
                qb.where((builder) => {
                    recursiveFilters(builder, filter.group_conditions)
                })
            } else {
                var func = logicGatesFilter[filter.gate_logic_previous] ? logicGatesFilter[filter.gate_logic_previous](qb, (builder) => {
                    recursiveFilters(builder, filter.group_conditions)
                }) : false
            }
        } else {
            if (filter.gate_logic_previous == null) {
                qb.where(getParam(0, filter.params), typeFilters[filter.type], getParam(1, filter.params))
            } else {
                logicGatesFilter[filter.gate_logic_previous] ? logicGatesFilter[filter.gate_logic_previous](qb, undefined, getParam(0, filter.params), typeFilters[filter.type], getParam(1, filter.params)) : false
            }
        }
    })
}

function recursiveFilters(builder, filters) {
    filters.forEach(filter => {
        var func = logicGatesFilter[filter.gate_logic_previous] ? true : false
        if (func) {
            logicGatesFilter[filter.gate_logic_previous](builder, undefined, getParam(0, filter.params), typeFilters[filter.type], getParam(1, filter.params))
        } else {
            builder.where(getParam(0, filter.params), typeFilters[filter.type], getParam(1, filter.params))
        }
    })
}

function getParam(idx, filterParams, as) {
    var param = filterParams[idx]
    if (param.type == "COLUMN") {
        var name = ""
        if (param.table_column.alias) {
            if (param.table_column.column_alias) {
                if (as) {
                    name = knex.raw(`"${param.table_column.alias}"."??" as "??"`, [knex.raw(param.table_column.column_name), knex.raw(as)])
                } else {
                    name = knex.raw(`"${param.table_column.alias}"."??"`, [knex.raw(param.table_column.column_name)])
                }
            } else {
                name += `${param.table_column.alias}.${param.table_column.column_name}`
            }
        } else {
            name += param.table_column.schema_name != null ? `${param.table_column.schema_name}.` : ""
            name += param.table_column.table_name != null ? `${param.table_column.table_name}.${param.table_column.column_name}` : param.table_column.column_name
        }
        return knex.raw(name)
    } else {
        if (param.param.type_input != "FUNCTION") {
            return knex.raw(`?${typesParamFilter[param.param.type_input]}`, [param.param.value])
        } else {
            return knex.raw(functionsParamFilter[param.param.input_functions])
        }
    }
}

function getOrders(qb, orders) {
    var orderArray = []
    orders.forEach(order => {
        var name = ""
        if (order.alias) {
            if (order.column_alias) {
                name = knex.raw(`"${order.alias}"."??"`, [knex.raw(order.column_name)])
            } else {
                name += `${order.alias}.${order.column_name}`
            }
        } else {
            name += order.schema_name != null ? `${order.schema_name}.` : ""
            order.table_name != null ? `${order.table_name}.${order.column_name}` : order.column_name
        }
        orderArray.push({
            column: name,
            order: order.type == "DESC" ? "desc" : "asc"
        })
    })
    qb.orderBy(orderArray)
}

function getTables(qb, table, queryTables) {
    console.log("Tables: ", table)
    tablesGenerated = []
    tablesJoins = []
    var name = ""
    name += table.schema_name != null ? `${table.schema_name}.` : ""
    name += table.table_name != null ? `${table.table_name}` : ""
    name += table.alias != null ? ` AS ${table.alias}` : ""
    console.log("Table name", name)
    qb.from(name)
    tablesGenerated.push(name)
    var tableName = table.alias != null ? `${table.alias}` : name
    getJoinTables(table.join, tableName, tablesGenerated, tablesJoins, queryTables)
    tablesJoins.forEach(join => {
        joinsType[join.type](qb, join)
    })

    console.log("Tables generated: ", tablesGenerated)
    console.log("Joins generated", tablesJoins)
}

function getJoinTables(joinObject, tableName, arrayTables, arrayJoins, queryTables) {
    if (joinObject == null) { return }
    var name = ""
    if (joinObject.table.query_code != null) {
        name += `(${queryTables[joinObject.table.query_code]})`
    } else {
        name += joinObject.table.schema_name != null ? `${joinObject.table.schema_name}.` : ""
        name += joinObject.table.table_name != null ? `${joinObject.table.table_name}` : ""
    }
    name += joinObject.table.alias != null ? ` AS ${joinObject.table.alias}` : ""
    var newTableName = joinObject.table.alias != null ? `${joinObject.table.alias}` : name
    let [aliasOrTableR, ...columnR] = joinObject.join_conditional.column_right.split(".")
    let right = knex.raw(`"??"."??"`, [knex.raw(aliasOrTableR), knex.raw(columnR.join("."))])
    let [aliasOrTableL, ...columnL] = joinObject.join_conditional.column_left.split(".")
    let left = knex.raw(`"??"."??"`, [knex.raw(aliasOrTableL), knex.raw(columnL.join("."))])
    arrayJoins.push(
        {
            type: joinObject.type,
            rigthTable: joinObject.table.query_code != null ? knex.raw(name) : name,
            columnLeft: left,
            columnRigth: right
        }
    )
    getJoinTables(joinObject.join, newTableName, arrayTables, arrayJoins, queryTables)
}

module.exports = { createQuery }