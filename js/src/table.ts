// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import { RecordBatch } from './recordbatch';
import { Col, Predicate } from './predicate';
import { Schema, Field, Struct } from './type';
import { read, readAsync } from './ipc/reader/arrow';
import { isPromise, isAsyncIterable } from './util/compat';
import { Vector, DictionaryVector, IntVector } from './vector';

export type NextFunc = (idx: number, cols: RecordBatch) => void;

export interface DataFrame {
    filter(predicate: Predicate): DataFrame;
    scan(next: NextFunc): void;
    count(): number;
    countBy(col: (Col|string)): CountByResult;
}

export class Table implements DataFrame {
    static empty() { return new Table(new Schema([]), []); }
    static from(sources?: Iterable<Uint8Array | Buffer | string> | object | string) {
        if (sources) {
            let schema: Schema | undefined;
            let recordBatches: RecordBatch[] = [];
            for (let recordBatch of read(sources)) {
                schema = schema || recordBatch.schema;
                recordBatches.push(recordBatch);
            }
            return new Table(schema || new Schema([]), recordBatches);
        }
        return Table.empty();
    }
    static async fromAsync(sources?: AsyncIterable<Uint8Array | Buffer | string>) {
        if (isAsyncIterable(sources)) {
            let schema: Schema | undefined;
            let recordBatches: RecordBatch[] = [];
            for await (let recordBatch of readAsync(sources)) {
                schema = schema || recordBatch.schema;
                recordBatches.push(recordBatch);
            }
            return new Table(schema || new Schema([]), recordBatches);
        } else if (isPromise(sources)) {
            return Table.from(await sources);
        } else if (sources) {
            return Table.from(sources);
        }
        return Table.empty();
    }

    public readonly schema: Schema;
    public readonly length: number;
    public readonly numCols: number;
    // List of inner RecordBatches
    public readonly batches: RecordBatch[];
    // List of inner Vectors, possibly spanning batches
    public readonly columns: Vector<any>[];
    // Union of all inner RecordBatches into one RecordBatch, possibly chunked.
    // If the Table has just one inner RecordBatch, this points to that.
    // If the Table has multiple inner RecordBatches, then this is a Chunked view
    // over the list of RecordBatches. This allows us to delegate the responsibility
    // of indexing, iterating, slicing, and visiting to the Nested/Chunked Data/Views.
    public readonly batchesUnion: RecordBatch;

    constructor(batches: RecordBatch[]);
    constructor(...batches: RecordBatch[]);
    constructor(schema: Schema, batches: RecordBatch[]);
    constructor(schema: Schema, ...batches: RecordBatch[]);
    constructor(...args: any[]) {
        let schema: Schema;
        let batches: RecordBatch[];
        if (args[0] instanceof Schema) {
            schema = args[0];
            batches = Array.isArray(args[1][0]) ? args[1][0] : args[1];
        } else if (args[0] instanceof RecordBatch) {
            schema = (batches = args)[0].schema;
        } else {
            schema = (batches = args[0])[0].schema;
        }
        this.schema = schema;
        this.batches = batches;
        this.batchesUnion = batches.reduce((union, batch) => union.concat(batch));
        this.columns = batches.slice(1).reduce((columns, batch) =>
            columns.map((col, idx) => col.concat(batch.columns[idx])),
            batches[0].columns
        );
        this.length = this.batchesUnion.length;
        this.numCols = this.batchesUnion.numCols;
    }
    public get(index: number): Struct['TValue'] {
        return this.batchesUnion.get(index)!;
    }
    public getColumn(name: string) {
        return this.getColumnAt(this.getColumnIndex(name));
    }
    public getColumnAt(index: number) {
        return this.columns[index];
    }
    public getColumnIndex(name: string) {
        return this.schema.fields.findIndex((f) => f.name === name);
    }
    public [Symbol.iterator](): IterableIterator<Struct['TValue']> {
        return this.batchesUnion[Symbol.iterator]() as any;
    }
    public filter(predicate: Predicate): DataFrame {
        return new FilteredDataFrame(this.batches, predicate);
    }
    public scan(next: NextFunc) {
        const batches = this.batches, numBatches = batches.length;
        for (let batchIndex = -1; ++batchIndex < numBatches;) {
            // load batches
            const batch = batches[batchIndex];
            // yield all indices
            for (let index = -1, numRows = batch.length; ++index < numRows;) {
                next(index, batch);
            }
        }
    }
    public count(): number { return this.length; }
    public countBy(name: Col | string): CountByResult {
        const batches = this.batches, numBatches = batches.length;
        const count_by = typeof name === 'string' ? new Col(name) : name;
        // Assume that all dictionary batches are deltas, which means that the
        // last record batch has the most complete dictionary
        count_by.bind(batches[numBatches - 1]);
        const vector = count_by.vector as DictionaryVector;
        if (!(vector instanceof DictionaryVector)) {
            throw new Error('countBy currently only supports dictionary-encoded columns');
        }
        // TODO: Adjust array byte width based on overall length
        // (e.g. if this.length <= 255 use Uint8Array, etc...)
        const counts: Uint32Array = new Uint32Array(vector.dictionary.length);
        for (let batchIndex = -1; ++batchIndex < numBatches;) {
            // load batches
            const batch = batches[batchIndex];
            // rebind the countBy Col
            count_by.bind(batch);
            const keys = (count_by.vector as DictionaryVector).indicies;
            // yield all indices
            for (let index = -1, numRows = batch.length; ++index < numRows;) {
                let key = keys.get(index);
                if (key !== null) { counts[key]++; }
            }
        }
        return new CountByResult(vector.dictionary, IntVector.from(counts));
    }
    public select(...columnNames: string[]) {
        return new Table(this.batches.map((batch) => batch.select(...columnNames)));
    }
    public toString(separator?: string) {
        let str = '';
        for (const row of this.rowsToString(separator)) {
            str += row + '\n';
        }
        return str;
    }
    public rowsToString(separator = ' | '): TableToStringIterator {
        return new TableToStringIterator(tableRowsToString(this, separator));
    }
}

class FilteredDataFrame implements DataFrame {
    private predicate: Predicate;
    private batches: RecordBatch[];
    constructor (batches: RecordBatch[], predicate: Predicate) {
        this.batches = batches;
        this.predicate = predicate;
    }
    public scan(next: NextFunc) {
        // inlined version of this:
        // this.parent.scan((idx, columns) => {
        //     if (this.predicate(idx, columns)) next(idx, columns);
        // });
        const batches = this.batches;
        const numBatches = batches.length;
        for (let batchIndex = -1; ++batchIndex < numBatches;) {
            // load batches
            const batch = batches[batchIndex];
            const predicate = this.predicate.bind(batch);
            // yield all indices
            for (let index = -1, numRows = batch.length; ++index < numRows;) {
                if (predicate(index, batch)) { next(index, batch); }
            }
        }
    }
    public count(): number {
        // inlined version of this:
        // let sum = 0;
        // this.parent.scan((idx, columns) => {
        //     if (this.predicate(idx, columns)) ++sum;
        // });
        // return sum;
        let sum = 0;
        const batches = this.batches;
        const numBatches = batches.length;
        for (let batchIndex = -1; ++batchIndex < numBatches;) {
            // load batches
            const batch = batches[batchIndex];
            const predicate = this.predicate.bind(batch);
            // yield all indices
            for (let index = -1, numRows = batch.length; ++index < numRows;) {
                if (predicate(index, batch)) { ++sum; }
            }
        }
        return sum;
    }
    public filter(predicate: Predicate): DataFrame {
        return new FilteredDataFrame(
            this.batches,
            this.predicate.and(predicate)
        );
    }
    public countBy(name: Col | string): CountByResult {
        const batches = this.batches, numBatches = batches.length;
        const count_by = typeof name === 'string' ? new Col(name) : name;
        // Assume that all dictionary batches are deltas, which means that the
        // last record batch has the most complete dictionary
        count_by.bind(batches[numBatches - 1]);
        const vector = count_by.vector as DictionaryVector;
        if (!(vector instanceof DictionaryVector)) {
            throw new Error('countBy currently only supports dictionary-encoded columns');
        }
        // TODO: Adjust array byte width based on overall length
        // (e.g. if this.length <= 255 use Uint8Array, etc...)
        const counts: Uint32Array = new Uint32Array(vector.dictionary.length);
        for (let batchIndex = -1; ++batchIndex < numBatches;) {
            // load batches
            const batch = batches[batchIndex];
            const predicate = this.predicate.bind(batch);
            // rebind the countBy Col
            count_by.bind(batch);
            const keys = (count_by.vector as DictionaryVector).indicies;
            // yield all indices
            for (let index = -1, numRows = batch.length; ++index < numRows;) {
                let key = keys.get(index);
                if (key !== null && predicate(index, batch)) { counts[key]++; }
            }
        }
        return new CountByResult(vector.dictionary, IntVector.from(counts));
    }
}

export class CountByResult extends Table implements DataFrame {
    constructor(values: Vector, counts: IntVector<any>) {
        super(
            new RecordBatch(new Schema([
                new Field('values', values.type),
                new Field('counts', counts.type)
            ]),
            counts.length, [values, counts]
        ));
    }
    public toJSON(): Object {
        const [values, counts] = this.columns;
        const result = {} as { [k: string]: number | null };
        for (let i = -1; ++i < this.length;) {
            result[values.get(i)] = counts.get(i);
        }
        return result;
    }
}

export class TableToStringIterator implements IterableIterator<string> {
    constructor(private iterator: IterableIterator<string>) {}
    [Symbol.iterator]() { return this.iterator; }
    next(value?: any) { return this.iterator.next(value); }
    throw(error?: any) { return this.iterator.throw && this.iterator.throw(error) || { done: true, value: '' }; }
    return(value?: any) { return this.iterator.return && this.iterator.return(value) || { done: true, value: '' }; }
    pipe(stream: NodeJS.WritableStream) {
        let res: IteratorResult<string>;
        let write = () => {
            if (stream.writable) {
                do {
                    if ((res = this.next()).done) { break; }
                } while (stream.write(res.value + '\n', 'utf8'));
            }
            if (!res || !res.done) {
                stream.once('drain', write);
            } else if (!(stream as any).isTTY) {
                stream.end('\n');
            }
        };
        write();
    }
}

function* tableRowsToString(table: Table, separator = ' | ') {
    const fields = table.schema.fields;
    const header = ['row_id', ...fields.map((f) => `${f}`)].map(stringify);
    const maxColumnWidths = header.map(x => x.length);
    // Pass one to convert to strings and count max column widths
    for (let i = -1, n = table.length - 1; ++i < n;) {
        let val, row = [i, ...table.get(i)];
        for (let j = -1, k = row.length; ++j < k; ) {
            val = stringify(row[j]);
            maxColumnWidths[j] = Math.max(maxColumnWidths[j], val.length);
        }
    }
    yield header.map((x, j) => leftPad(x, ' ', maxColumnWidths[j])).join(separator);
    for (let i = -1, n = table.length; ++i < n;) {
        yield [i, ...table.get(i)]
            .map((x) => stringify(x))
            .map((x, j) => leftPad(x, ' ', maxColumnWidths[j]))
            .join(separator);
    }
}

function leftPad(str: string, fill: string, n: number) {
    return (new Array(n + 1).join(fill) + str).slice(-1 * n);
}

function stringify(x: any) {
    return typeof x === 'string' ? `"${x}"` : ArrayBuffer.isView(x) ? `[${x}]` : JSON.stringify(x);
}
