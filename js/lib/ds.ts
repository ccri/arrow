import * as arrow from './arrow';
import * as e from './expr';
import * as fs from 'fs';
import * as types from './types';

export class ArrowDataSet {

    reader: arrow.ArrowReader;
    ds: types.Vector;

    constructor(public url: string, name: string) {
        this.reader = arrow.getReader(fs.readFileSync(url));
        this.ds = this.reader.getVector(name);
        this.reader.loadNextBatch();
    };

    query(expr: e.Expr): Array<number> {
        expr.bind(this.ds);
        this.reader.setBatchIndex(0);
        var numBatches = this.reader.getBatchCount();
        var res = [];
        var k = 0;
        for(var j=0; j<numBatches; j++) {
            var batchCount = this.reader.loadNextBatch();
            var cols = this.ds.getChildVectors().map(function(c) { return c.dataView; });
            for(var i=0; i<batchCount; i++) {
                if(expr.eval(i, cols) == true) {
                    res.push(k);
                }
                k = k+1;
            }
        }
        return res;
    };

    countBy(expr: e.Expr): {} {
        expr.bind(this.ds);
        var ret = {};
        this.reader.setBatchIndex(0);
        var numBatches = this.reader.getBatchCount();
        var k = 0;
        for(var j=0; j<numBatches; j++) {
            var batchCount = this.reader.loadNextBatch();
            var cols = this.ds.getChildVectors().map(function(c) { return c.dataView; });
            for(var i=0; i<batchCount; i++) {
                var key = expr.eval(i, cols);
                ret[key] = ret[key] === undefined ? 1 : ret[key]+1;
            }
        }
        return ret;
    }
}
