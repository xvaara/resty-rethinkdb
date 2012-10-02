goog.provide('rethinkdb.Table');

goog.require('rethinkdb');
goog.require('rethinkdb.Expression');

/**
 * @class A table in a database
 * @param {string} tableName
 * @param {string} dbName
 * @param {boolean=} opt_allowOutdated
 * @constructor
 * @extends {rethinkdb.Expression}
 */
rethinkdb.Table = function(tableName, dbName, opt_allowOutdated) {
    this.name_ = tableName;
    this.db_ = dbName;
    this.allowOutdated_ = (typeof opt_allowOutdated === 'boolean') ?
        opt_allowOutdated : null;
};
goog.inherits(rethinkdb.Table, rethinkdb.Expression);

/**
 * @override
 * @ignore
 */
rethinkdb.Table.prototype.compile = function(opt_buildOpts) {
    var term = new Term();
    term.setType(Term.TermType.TABLE);

    var table = new Term.Table();
    var tableRef = new TableRef();
    tableRef.setDbName(this.db_ || rethinkdb.last_connection_.getDefaultDb());
    tableRef.setTableName(this.name_);

    var allowOutdated;
    if (this.allowOutdated_ !== null) {
        allowOutdated = this.allowOutdated;
    } else {
        if (opt_buildOpts && (opt_buildOpts.defaultAllowOutdated !== undefined)) {
            allowOutdated = opt_buildOpts.defaultAllowOutdated;
        } else {
            allowOutdated = false;
        }
    }
    tableRef.setUseOutdated(allowOutdated);

    table.setTableRef(tableRef);

    term.setTable(table);
    return term;
};

/** @override */
rethinkdb.Table.prototype.formatQuery = function(bt) {
    if (!bt) {
        if (this.db_) {
            return "r.db('"+this.db_+"').table('"+this.name_+"')";
        } else {
            return "r.table('"+this.name_+"')";
        }
    } else {
        if (this.db_) {
            return carrotify_("r.db('"+this.db_+"').table('"+this.name_+"')");
        } else {
            return carrotify_("r.table('"+this.name_+"')");
        }
    }
};

/**
 * @constructor
 * @extends {rethinkdb.Expression}
 * @ignore
 */
rethinkdb.GetExpression = function(table, key, opt_primaryKey) {
    this.table_ = table;
    this.key_ = key;
    this.primaryKey_ = opt_primaryKey || 'id';
};
goog.inherits(rethinkdb.GetExpression, rethinkdb.Expression);

/** @override */
rethinkdb.GetExpression.prototype.compile = function(opt_buildOpts) {
    var tableTerm = this.table_.compile(opt_buildOpts);
    var tableRef = tableTerm.getTable().getTableRefOrDefault();

    var get = new Term.GetByKey();
    get.setTableRef(tableRef);
    get.setAttrname(this.primaryKey_);
    get.setKey(this.key_.compile(opt_buildOpts));

    var term = new Term();
    term.setType(Term.TermType.GETBYKEY);
    term.setGetByKey(get);

    return term;
};

/** @override */
rethinkdb.GetExpression.prototype.formatQuery = function(bt) {
    if (!bt) {
        return this.table_.formatQuery()+".get("+this.key_.formatQuery()+", '"+this.primaryKey_+"')";
    } else {
        var a = bt.shift();
        if (a === 'key') {
            return spaceify_(this.table_.formatQuery()+".get(")+
                this.key_.formatQuery(bt)+spaceify_(", '"+this.primaryKey_+"')");
        } else if (a === 'attrname') {
            return spaceify_(this.table_.formatQuery()+".get("+
                this.key_.formatQuery()+", ")+carrotify_("'"+this.primaryKey_+"'")+" ";
        } else {
            goog.asserts.assert(a === undefined);
            return this.table_.formatQuery(bt)+spaceify_(".get("+
                this.key_.formatQuery()+", '"+this.primaryKey_+"')");
        }
    }
};

/**
 * Return a single row of this table by key
 * @param {*} key
 * @param {string=} opt_primaryKey
 */
rethinkdb.Table.prototype.get = function(key, opt_primaryKey) {
    argCheck_(arguments, 1);
    key = wrapIf_(key);
    return newExpr_(rethinkdb.GetExpression, this, key, opt_primaryKey);
};
goog.exportProperty(rethinkdb.Table.prototype, 'get',
                    rethinkdb.Table.prototype.get);

/**
 * @param {rethinkdb.Table} table
 * @param {Array.<rethinkdb.Expression>} docs
 * @constructor
 * @extends {rethinkdb.Query}
 * @ignore
 */
rethinkdb.InsertQuery = function(table, docs) {
    this.table_ = table;
    this.docs_ = docs;
};
goog.inherits(rethinkdb.InsertQuery, rethinkdb.Query);

/** @override */
rethinkdb.InsertQuery.prototype.buildQuery = function(opt_buildOpts) {
    var tableTerm = this.table_.compile(opt_buildOpts);
    var tableRef = tableTerm.getTable().getTableRefOrDefault();

    var insert = new WriteQuery.Insert();
    insert.setTableRef(tableRef);

    for (var i = 0; i < this.docs_.length; i++) {
        insert.addTerms(this.docs_[i].compile(opt_buildOpts));
    }

    var writeQuery = new WriteQuery();
    writeQuery.setType(WriteQuery.WriteQueryType.INSERT);
    writeQuery.setInsert(insert);

    var query = new Query();
    query.setType(Query.QueryType.WRITE);
    query.setWriteQuery(writeQuery);

    return query;
};

/** @override */
rethinkdb.InsertQuery.prototype.formatQuery = function(bt) {
    var docs = this.docs_.map(function(a) {return a.formatQuery()});

    if (!bt) {
        var docsStr = "["+docs.join(', ')+"]";
        return this.table_.formatQuery()+".insert("+docsStr+")";
    } else {
        docs = docs.map(spaceify_);
        var a = bt.shift();
        var tab
        if (a !== undefined) {
            tab = spaceify_(this.table_.formatQuery());
            var b = a.split(':');
            goog.asserts.assert(b[0] === 'term');
            var i = parseInt(b[1], 10);
            docs[i] = this.docs_[i].formatQuery(bt);
        } else {
            tab = this.table_.formatQuery(bt);
        }

        var docsStr = " "+docs.join('  ')+" ";
        return tab+"        "+docsStr+" ";
    }
};

/**
 * Insert a json document into this table
 * @param {*} docs An object or list of objects to insert
 */
rethinkdb.Table.prototype.insert = function(docs) {
    argCheck_(arguments, 1);
    if (!goog.isArray(docs))
        docs = [docs];
    docs = docs.map(rethinkdb.expr);
    return new rethinkdb.InsertQuery(this, docs);
};
goog.exportProperty(rethinkdb.Table.prototype, 'insert',
                    rethinkdb.Table.prototype.insert);

/**
 * @param {rethinkdb.Expression} view
 * @constructor
 * @extends {rethinkdb.Query}
 * @ignore
 */
rethinkdb.DeleteQuery = function(view) {
    this.view_ = view;
};
goog.inherits(rethinkdb.DeleteQuery, rethinkdb.Query);

/** @override */
rethinkdb.DeleteQuery.prototype.buildQuery = function(opt_buildOpts) {
    var del = new WriteQuery.Delete();
    del.setView(this.view_.compile(opt_buildOpts));

    var write = new WriteQuery();
    write.setType(WriteQuery.WriteQueryType.DELETE);
    write.setDelete(del);

    var query = new Query();
    query.setType(Query.QueryType.WRITE);
    query.setWriteQuery(write);

    return query;
};

/** @override */
rethinkdb.DeleteQuery.prototype.formatQuery = function(bt) {
    if (!bt) {
        return this.view_.formatQuery()+".del()";
    } else {
        var a = bt.shift();
        goog.asserts.assert(a === 'view');
        return this.view_.formatQuery(bt)+"      ";
    }
};

/**
 * @param {rethinkdb.Table} table
 * @param {rethinkdb.Expression} key
 * @constructor
 * @extends {rethinkdb.Query}
 * @ignore
 */
rethinkdb.PointDeleteQuery = function(table, key, primaryKey) {
    this.table_ = table;
    this.key_ = key;
    this.primaryKey_ = primaryKey || 'id';
};
goog.inherits(rethinkdb.PointDeleteQuery, rethinkdb.Query);

/** @override */
rethinkdb.PointDeleteQuery.prototype.buildQuery = function(opt_buildOpts) {
    var pointdelete = new WriteQuery.PointDelete();
    pointdelete.setTableRef(this.table_.compile(opt_buildOpts).getTableOrDefault().getTableRefOrDefault());

    pointdelete.setAttrname(this.primaryKey_);
    pointdelete.setKey(this.key_.compile(opt_buildOpts));

    var write = new WriteQuery();
    write.setType(WriteQuery.WriteQueryType.POINTDELETE);
    write.setPointDelete(pointdelete);

    var query = new Query();
    query.setType(Query.QueryType.WRITE);
    query.setWriteQuery(write);

    return query;
};

/** @override */
rethinkdb.PointDeleteQuery.prototype.formatQuery = function(bt) {
    if (!bt) {
        return this.table_.formatQuery()+".get("+this.key_.formatQuery()+").del()";
    } else {
        var a = bt.shift();
        if (a !== undefined) {
            goog.asserts.assert(a === 'key');
            return spaceify_(this.table_.formatQuery()+".get(")+this.key_.formatQuery(bt)+"       ";
        } else {
            return this.table_.formatQuery(bt)+spaceify_(".get("+this.key_.formatQuery()+").del()");
        }
    }
};

/**
 * Deletes all rows the current view
 */
rethinkdb.Expression.prototype.del = function() {
    if (this instanceof rethinkdb.GetExpression) {
        return new rethinkdb.PointDeleteQuery(this.table_, this.key_, this.primaryKey_);
    } else {
        return new rethinkdb.DeleteQuery(this);
    }
};
goog.exportProperty(rethinkdb.Expression.prototype, 'del',
                    rethinkdb.Expression.prototype.del);

/**
 * @param {rethinkdb.Expression} view
 * @param {rethinkdb.FunctionExpression} mapping
 * @constructor
 * @extends {rethinkdb.Query}
 * @ignore
 */
rethinkdb.UpdateQuery = function(view, mapping) {
    this.view_ = view;
    this.mapping_ = mapping;
};
goog.inherits(rethinkdb.UpdateQuery, rethinkdb.Query);

/** @override */
rethinkdb.UpdateQuery.prototype.buildQuery = function(opt_buildOpts) {
    var mapping = new Mapping();
    mapping.setArg(this.mapping_.args[0]);
    mapping.setBody(this.mapping_.body.compile(opt_buildOpts));

    var update = new WriteQuery.Update();
    update.setView(this.view_.compile(opt_buildOpts));
    update.setMapping(mapping);

    var write = new WriteQuery();
    write.setType(WriteQuery.WriteQueryType.UPDATE);
    write.setUpdate(update);

    var query = new Query();
    query.setType(Query.QueryType.WRITE);
    query.setWriteQuery(write);

    return query;
};

/** @override */
rethinkdb.UpdateQuery.prototype.formatQuery = function(bt) {
    if (!bt) {
        return this.view_.formatQuery()+".update("+this.mapping_.formatQuery()+")";
    } else {
        var a = bt.shift();
        if (a === 'view') {
            return this.view_.formatQuery(bt)+spaceify_(".update("+this.mapping_.formatQuery()+")");
        } else {
            goog.asserts.assert(a === 'mapping');
            return spaceify_(this.view_.formatQuery()+".update(")+this.mapping_.formatQuery(bt)+" ";
        }
    }
};

/**
 * @param {rethinkdb.Table} table
 * @param {rethinkdb.Expression} key
 * @param {rethinkdb.FunctionExpression} mapping
 * @constructor
 * @extends {rethinkdb.Query}
 * @ignore
 */
rethinkdb.PointUpdateQuery = function(table, key, primaryKey, mapping) {
    this.table_ = table;
    this.key_ = key;
    this.primaryKey_ = primaryKey || 'id';
    this.mapping_ = mapping;
};
goog.inherits(rethinkdb.PointUpdateQuery, rethinkdb.Query);

/** @override */
rethinkdb.PointUpdateQuery.prototype.buildQuery = function(opt_buildOpts) {
    var mapping = new Mapping();
    mapping.setArg(this.mapping_.args[0]);
    mapping.setBody(this.mapping_.body.compile(opt_buildOpts));

    var pointupdate = new WriteQuery.PointUpdate();
    pointupdate.setTableRef(this.table_.compile(opt_buildOpts).getTableOrDefault().getTableRefOrDefault());
    pointupdate.setAttrname(this.primaryKey_);
    pointupdate.setKey(this.key_.compile(opt_buildOpts));
    pointupdate.setMapping(mapping);

    var write = new WriteQuery();
    write.setType(WriteQuery.WriteQueryType.POINTUPDATE);
    write.setPointUpdate(pointupdate);

    var query = new Query();
    query.setType(Query.QueryType.WRITE);
    query.setWriteQuery(write);

    return query;
};

/** @override */
rethinkdb.PointUpdateQuery.prototype.formatQuery = function(bt) {
    if (!bt) {
        return this.table_.formatQuery()+".get("+this.key_.formatQuery()+", '"+this.primaryKey_+
            "').update("+this.mapping_.formatQuery()+")";
    } else {
        var a = bt.shift();
        if (a === undefined) {
            return this.table_.formatQuery(bt)+spaceify_(".get("+this.key_.formatQuery()+
                ", '"+this.primaryKey_+"').update("+this.mapping_.formatQuery()+")");
        } else if (a === 'key') {
            return spaceify_(this.table_.formatQuery()+".get(")+this.key_.formatQuery(bt)+
                    spaceify_(", '"+this.primaryKey_+"').update("+this.mapping_.formatQuery()+")");
        } else if (a === 'keyname') {
            return spaceify_(this.table_.formatQuery()+".get("+this.key_.formatQuery()+
                    ", ")+carrotify_("'"+this.primaryKey_+"'")+spaceify_(").update("+
                    this.mapping_.formatQuery()+")");
        } else {
            goog.asserts.assert(a === 'point_map');
            return spaceify_(this.table_.formatQuery()+".get("+this.key_.formatQuery()+", '"+
                    this.primaryKey_+"').update(")+this.mapping_.formatQuery(bt)+" ";
        }
    }
};

/**
 * Updates each row in the current view by merging in the result
 * of the given mapping function applied to that row
 * @param {function(...)|rethinkdb.FunctionExpression|rethinkdb.Expression} mapping
 */
rethinkdb.Expression.prototype.update = function(mapping) {
    argCheck_(arguments, 1);

    mapping = functionWrap_(mapping);

    if (this instanceof rethinkdb.GetExpression) {
        return new rethinkdb.PointUpdateQuery(this.table_, this.key_, this.primaryKey_, mapping);
    } else {
        return new rethinkdb.UpdateQuery(this, mapping);
    }
};
goog.exportProperty(rethinkdb.Expression.prototype, 'update',
                    rethinkdb.Expression.prototype.update);

/**
 * @param {rethinkdb.Expression} view
 * @param {rethinkdb.FunctionExpression} mapping
 * @constructor
 * @extends {rethinkdb.Query}
 * @ignore
 */
rethinkdb.MutateQuery = function(view, mapping) {
    this.view_ = view;
    this.mapping_ = mapping;
};
goog.inherits(rethinkdb.MutateQuery, rethinkdb.Query);

/** @override */
rethinkdb.MutateQuery.prototype.buildQuery = function(opt_buildOpts) {
    var mapping = new Mapping();
    mapping.setArg(this.mapping_.args[0]);
    mapping.setBody(this.mapping_.body.compile(opt_buildOpts));

    var mutate = new WriteQuery.Mutate();
    mutate.setView(this.view_.compile(opt_buildOpts));
    mutate.setMapping(mapping);

    var write = new WriteQuery();
    write.setType(WriteQuery.WriteQueryType.MUTATE);
    write.setMutate(mutate);

    var query = new Query();
    query.setType(Query.QueryType.WRITE);
    query.setWriteQuery(write);

    return query;
};

/** @override */
rethinkdb.MutateQuery.prototype.formatQuery = function(bt) {
    if (!bt) {
        return this.view_.formatQuery()+".mutate("+this.mapping_.formatQuery()+")";
    } else {
        var a = bt.shift();
        if (a === 'view') {
            return this.view_.formatQuery(bt)+spaceify_(".mutate("+this.mapping_.formatQuery()+")");
        } else {
            goog.asserts.assert(a === 'mapping');
            return spaceify_(this.view_.formatQuery()+".mutate(")+this.mapping_.formatQuery(bt)+" ";
        }
    }
};

/**
 * @param {rethinkdb.Table} table
 * @param {rethinkdb.Expression} key
 * @param {rethinkdb.FunctionExpression} mapping
 * @constructor
 * @extends {rethinkdb.Query}
 * @ignore
 */
rethinkdb.PointMutateQuery = function(table, key, primaryKey, mapping) {
    this.table_ = table;
    this.key_ = key;
    this.primaryKey_ = primaryKey || 'id';
    this.mapping_ = mapping;
};
goog.inherits(rethinkdb.PointMutateQuery, rethinkdb.Query);

/** @override */
rethinkdb.PointMutateQuery.prototype.buildQuery = function(opt_buildOpts) {
    var mapping = new Mapping();
    mapping.setArg(this.mapping_.args[0]);
    mapping.setBody(this.mapping_.body.compile(opt_buildOpts));

    var pointmutate = new WriteQuery.PointMutate();
    pointmutate.setTableRef(this.table_.compile(opt_buildOpts).getTableOrDefault().getTableRefOrDefault());
    pointmutate.setAttrname(this.primaryKey_);
    pointmutate.setKey(this.key_.compile(opt_buildOpts));
    pointmutate.setMapping(mapping);

    var write = new WriteQuery();
    write.setType(WriteQuery.WriteQueryType.POINTMUTATE);
    write.setPointMutate(pointmutate);

    var query = new Query();
    query.setType(Query.QueryType.WRITE);
    query.setWriteQuery(write);

    return query;
};

/** @override */
rethinkdb.PointMutateQuery.prototype.formatQuery = function(bt) {
    if (!bt) {
        return this.table_.formatQuery()+".get("+this.key_.formatQuery()+", '"+this.primaryKey_+
            "').mutate("+this.mapping_.formatQuery()+")";
    } else {
        var a = bt.shift();
        if (a === undefined) {
            return this.table_.formatQuery(bt)+spaceify_(".get("+this.key_.formatQuery()+
                ", '"+this.primaryKey_+"').mutate("+this.mapping_.formatQuery()+")");
        } else if (a === 'key') {
            return spaceify_(this.table_.formatQuery()+".get(")+this.key_.formatQuery(bt)+
                    spaceify_(", '"+this.primaryKey_+"').mutate("+this.mapping_.formatQuery()+")");
        } else if (a === 'keyname') {
            return spaceify_(this.table_.formatQuery()+".get("+this.key_.formatQuery()+
                    ", ")+carrotify_("'"+this.primaryKey_+"'")+spaceify_(").mutate("+
                    this.mapping_.formatQuery()+")");
        } else {
            goog.asserts.assert(a === 'point_map');
            return spaceify_(this.table_.formatQuery()+".get("+this.key_.formatQuery()+", '"+
                    this.primaryKey_+"').mutate(")+this.mapping_.formatQuery(bt)+" ";
        }
    }
};

/**
 * Replcaces each row of the current view with the result of the
 * mapping function as applied to the current row.
 * @param {function(...)|rethinkdb.FunctionExpression|rethinkdb.Expression} mapping
 */
rethinkdb.Expression.prototype.mutate = function(mapping) {
    argCheck_(arguments, 1);
    mapping = functionWrap_(mapping);
    if (this instanceof rethinkdb.GetExpression) {
        return new rethinkdb.PointMutateQuery(this.table_, this.key_, this.primaryKey_, mapping);
    } else {
        return new rethinkdb.MutateQuery(this, mapping);
    }
};
goog.exportProperty(rethinkdb.Expression.prototype, 'mutate',
                    rethinkdb.Expression.prototype.mutate);
