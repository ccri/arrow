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

import { flatbuffers } from 'flatbuffers';
import { BitArray } from './bitarray';
import { TextDecoder } from 'text-encoding';
import { org } from './Arrow_generated';

import ByteBuffer = flatbuffers.ByteBuffer;
var Type = org.apache.arrow.flatbuf.Type;

interface ArrayView {
    slice(start: number, end: number) : ArrayView
    toString() : string
}

export abstract class Vector {

    name: string;
    length: number;
    null_count: number;

    constructor(name: string) {
        this.name = name;
    }

    /* Access datum at index i */
    abstract get(i: number);

    /* Return array representing data in the range [start, end) */
    slice(start: number, end: number) {
        var result = [];
        for (var i = start; i < end; i += 1|0) {
            result.push(this.get(i));
        }
        return result;
    }

    /* Return array of child vectors, for container types */
    abstract getChildVectors();

    /**
     * Use recordBatch fieldNodes and Buffers to construct this Vector
     *   bb: flatbuffers.ByteBuffer
     *   node: org.apache.arrow.flatbuf.FieldNode
     *   buffers: { offset: number, length: number }[]
     */
    public loadData(bb: ByteBuffer, node, buffers) {
        this.length = node.length().low;
        this.null_count = node.nullCount().low;
        this.loadBuffers(bb, node, buffers);
    }

    /* Return the numbder of buffers read by loadBuffers */
    public abstract getLayoutLength(): number;

    protected abstract loadBuffers(bb: ByteBuffer, node, buffers);

    /**
     * Helper function for loading a VALIDITY buffer (for Nullable types)
     *   bb: flatbuffers.ByteBuffer
     *   buffer: org.apache.arrow.flatbuf.Buffer
     */
    static loadValidityBuffer(bb: ByteBuffer, buffer) : BitArray {
        var arrayBuffer = bb.bytes_.buffer;
        var offset = bb.bytes_.byteOffset + buffer.offset;
        return new BitArray(arrayBuffer, offset, buffer.length * 8);
    }

    /**
     * Helper function for loading an OFFSET buffer
     *   buffer: org.apache.arrow.flatbuf.Buffer
     */
    static loadOffsetBuffer(bb: ByteBuffer, buffer) : Vector {
        var vector = new Int32Vector("$offsets");
        vector.loadBuffers(bb, null, [buffer]);
        return vector;
    }
}

abstract class SimpleVector extends Vector {
    buffer: ByteBuffer;

    getChildVectors() {
        return [];
    }

    public getLayoutLength(): number { return 1; }

    loadBuffers(bb: ByteBuffer, node, buffers) {
        this.loadDataBuffer(bb, buffers[0]);
    }

    /**
      * buffer: org.apache.arrow.flatbuf.Buffer
      */
    protected loadDataBuffer(bb: ByteBuffer, buffer) {
        // use subarray to avoid copying the underlying buffer
        this.buffer = new ByteBuffer(bb.bytes_.subarray(buffer.offset, buffer.offset + buffer.length));
    }

    getBuffer() {
        return this.buffer;
    }

    toString() {
        return this.buffer.toString();
    }
}

abstract class NullableSimpleVector extends SimpleVector {
    protected validityView: BitArray;

    get(i: number) {
        if (!this.null_count || this.validityView.get(i)) {
            return this._get(i);
        } else {
          return null;
        }
    }

    protected abstract _get(i: number)

    public getLayoutLength(): number { return 2; }

    loadBuffers(bb: ByteBuffer, node, buffers) {
        this.validityView = Vector.loadValidityBuffer(bb, buffers[0]);
        this.loadDataBuffer(bb, buffers[1]);
    }

    getValidityVector() {
        return this.validityView;
    }
}

class Uint8Vector   extends SimpleVector { get(i: number): any { return this.buffer.readUint8(i);      } }
class Uint16Vector  extends SimpleVector { get(i: number): any { return this.buffer.readUint16(i<<1);  } }
class Uint32Vector  extends SimpleVector { get(i: number): any { return this.buffer.readUint32(i<<2);  } }
class Uint64Vector  extends SimpleVector { get(i: number): any { return this.buffer.readUint64(i<<3);  } }
class Int8Vector    extends SimpleVector { get(i: number): any { return this.buffer.readInt8(i);       } }
class Int16Vector   extends SimpleVector { get(i: number): any { return this.buffer.readInt16(i<<1);   } }
class Int32Vector   extends SimpleVector { get(i: number): any { return this.buffer.readInt32(i<<2);   } }
class Int64Vector   extends SimpleVector { get(i: number): any { return this.buffer.readInt64(i<<3);   } }
class Float32Vector extends SimpleVector { get(i: number): any { return this.buffer.readFloat32(i<<2); } }
class Float64Vector extends SimpleVector { get(i: number): any { return this.buffer.readFloat64(i<<3); } }

class NullableUint8Vector   extends NullableSimpleVector { _get(i: number) { return this.buffer.readUint8(i);      } }
class NullableUint16Vector  extends NullableSimpleVector { _get(i: number) { return this.buffer.readUint16(i<<1);  } }
class NullableUint32Vector  extends NullableSimpleVector { _get(i: number) { return this.buffer.readUint32(i<<2);  } }
class NullableUint64Vector  extends NullableSimpleVector { _get(i: number) { return this.buffer.readUint64(i<<3);  } }
class NullableInt8Vector    extends NullableSimpleVector { _get(i: number) { return this.buffer.readInt8(i);       } }
class NullableInt16Vector   extends NullableSimpleVector { _get(i: number) { return this.buffer.readInt16(i<<1);   } }
class NullableInt32Vector   extends NullableSimpleVector { _get(i: number) { return this.buffer.readInt32(i<<2);   } }
class NullableInt64Vector   extends NullableSimpleVector { _get(i: number) { return this.buffer.readInt64(i<<3);   } }
class NullableFloat32Vector extends NullableSimpleVector { _get(i: number) { return this.buffer.readFloat32(i<<2); } }
class NullableFloat64Vector extends NullableSimpleVector { _get(i: number) { return this.buffer.readFloat64(i<<3); } }

class DateVector extends Uint32Vector { get (i) { return new Date(super.get(2*i+1)*Math.pow(2,32) + super.get(2*i)); } }

class NullableDateVector extends DateVector {
    private validityView: BitArray;

    public getLayoutLength(): number { return 2; }

    loadBuffers(bb, node, buffers) {
        this.validityView = Vector.loadValidityBuffer(bb, buffers[0]);
        this.loadDataBuffer(bb, buffers[1]);
    }

    get (i) {
        if (this.validityView.get(i)) {
            return super.get(i);
        } else {
            return null;
        }
    }

    getValidityVector() {
        return this.validityView;
    }
}

class Utf8Vector extends Vector {

    protected offsetView: Int32Vector;
    protected dataVector: Uint8Vector = new Uint8Vector('$data'); // TODO: this should probably be a Uint8Array we deal with directly. so we can slice it and pass it to the decoder
    static decoder: TextDecoder = new TextDecoder('utf8');

    getChildVectors() {
        return [];
    }

    public getLayoutLength(): number { return 1 + this.dataVector.getLayoutLength(); }

    loadBuffers(bb: ByteBuffer, node, buffers) {
        this.offsetView = Vector.loadOffsetBuffer(bb, buffers[0]) as Int32Vector;
        this.dataVector.loadBuffers(bb, node, buffers.slice(1));
    }

    get(i) {
        var from = this.offsetView.get(i)
        var to = this.offsetView.get(i + 1)
        // use subarray to avoid copying the underlying buffer
        return Utf8Vector.decoder.decode(this.dataVector.buffer.bytes_.subarray(from, to));
    }

    slice(start: number, end: number) {
        var result: string[] = [];
        for (var i: number = start; i < end; i += 1|0) {
            result.push(this.get(i));
        }
        return result;
    }

    getOffsetView() {
        return this.offsetView;
    }
}

class NullableUtf8Vector extends Utf8Vector {
    private validityView: BitArray;

    public getLayoutLength(): number { return 1 + super.getLayoutLength(); }

    loadBuffers(bb, node, buffers) {
        this.validityView = Vector.loadValidityBuffer(bb, buffers[0]);
        this.offsetView = Vector.loadOffsetBuffer(bb, buffers[1]) as Int32Vector;
        this.dataVector.loadBuffers(bb, node, buffers.slice(2));
    }

    get(i) {
        if (this.validityView.get(i)) {
            return super.get(i);
        } else {
            return null;
        }
    }

    getValidityVector() {
        return this.validityView;
    }
}

// Nested Types
class ListVector extends Uint32Vector {
    private dataVector: Vector;

    constructor(name: string, dataVector: Vector) {
        super(name);
        this.dataVector = dataVector;
    }

    getChildVectors() {
        return [this.dataVector];
    }

    loadBuffers(bb, node, buffers) {
        super.loadBuffers(bb, node, buffers);
        this.length -= 1;
    }

    get(i) {
        var offset = super.get(i)
        if (offset === null) {
            return null;
        }
        var next_offset = super.get(i + 1)
        return this.dataVector.slice(offset, next_offset)
    }

    toString() {
        return "length: " + (this.length);
    }

    slice(start: number, end: number) {
        var result = [];
        for (var i = start; i < end; i += 1|0) {
            result.push(this.get(i));
        }
        return result;
    }
}

class NullableListVector extends ListVector {
    private validityView: BitArray;

    loadBuffers(bb, node, buffers) {
        this.validityView = Vector.loadValidityBuffer(bb, buffers[0]);
        this.loadDataBuffer(bb, buffers[1]);
        this.length -= 1;
    }

    get(i) {
        if (this.validityView.get(i)) {
            return super.get(i);
        } else {
            return null;
        }
    }

    getValidityVector() {
        return this.validityView;
    }
}

class FixedSizeListVector extends Vector {
    private size: number
    private dataVector: Vector;

    constructor(name: string, size: number, dataVector: Vector) {
        super(name);
        this.size = size;
        this.dataVector = dataVector;
    }

    getChildVectors() {
        return [this.dataVector];
    }

    public getLayoutLength(): number { return 0; }
    loadBuffers(bb, node, buffers) {
        // no buffers to load
    }

    get(i: number) {
        return this.dataVector.slice(i * this.size, (i + 1) * this.size);
    }

    getItem(elem_idx: number, item_idx: number) {
        return this.dataVector.get(elem_idx * this.size + item_idx);
    }

    slice(start : number, end : number) {
        var result = [];
        for (var i = start; i < end; i += 1|0) {
            result.push(this.get(i));
        }
        return result;
    }

    getListSize() {
        return this.size;
    }
}

class NullableFixedSizeListVector extends FixedSizeListVector {
    private validityView: BitArray;

    public getLayoutLength(): number { return 1; }
    loadBuffers(bb, node, buffers) {
        this.validityView = Vector.loadValidityBuffer(bb, buffers[0]);
    }

    get(i: number) {
        if (this.validityView.get(i)) {
            return super.get(i);
        } else {
            return null;
        }
    }

    getValidityVector() {
        return this.validityView;
    }
}

class StructVector extends Vector {
    private validityView: BitArray;
    private vectors: Vector[];
    private vectorsByName: {[name: string]: Vector}

    constructor(name: string, vectors: Vector[]) {
        super(name);
        this.vectors = vectors;
        this.vectorsByName = {};
        this.vectors.forEach((v: Vector) => { this.vectorsByName[v.name] = v; });
    }

    getChildVectors() {
        return this.vectors;
    }

    getVector(name: string): Vector {
        return this.vectorsByName[name];
    }

    public getLayoutLength(): number { return 1; }
    loadBuffers(bb, node, buffers) {
        this.validityView = Vector.loadValidityBuffer(bb, buffers[0]);
    }

    get(i : number) {
        if (this.validityView.get(i)) {
          return this.vectors.map((v: Vector) => v.get(i));
        } else {
            return null;
        }
    }

    slice(start : number, end : number) {
        var result = [];
        for (var i = start; i < end; i += 1|0) {
            result.push(this.get(i));
        }
        return result;
    }

    getValidityVector() {
        return this.validityView;
    }
}

export class DictionaryVector extends Vector {
    private indices: Vector;
    private dictionary: Vector;

    constructor (name: string, indices: Vector, dictionary: Vector) {
        super(name);
        this.indices = indices;
        this.dictionary = dictionary;
    }

    get(i) {
        return this.decode(this.getEncoded(i));
    }

    /** Get the dictionary encoded value */
    public getEncoded(i): number {
        return this.indices.get(i);
    }

    public decode(encoded: number) {
        if (encoded == null) {
            return null;
        } else {
            return this.dictionary.get(encoded);
        }
    }

    slice(start, end) {
        return this.indices.slice(start, end); // TODO decode
    }

    getChildVectors() {
        return this.indices.getChildVectors();
    }

    public getLayoutLength(): number { return this.indices.getLayoutLength(); }
    loadBuffers(bb, node, buffers) {
        this.indices.loadData(bb, node, buffers);
    }

    /** Get the index (encoded) vector */
    public getIndexVector() {
        return this.indices;
    }

    /** Get the dictionary vector */
    public getDictionaryVector() {
        return this.dictionary;
    }

    toString() {
        return this.indices.toString();
    }
}

export function vectorFromField(field, dictionaries) : Vector {
    var dictionary = field.dictionary(), nullable = field.nullable();
    var name = field.name();
    if (dictionary == null) {
        var typeType = field.typeType();
        if (typeType === Type.List) {
            var dataVector = vectorFromField(field.children(0), dictionaries);
            return nullable ? new NullableListVector(name, dataVector) : new ListVector(name, dataVector);
        } else if (typeType === Type.FixedSizeList) {
            var dataVector = vectorFromField(field.children(0), dictionaries);
            var size = field.type(new org.apache.arrow.flatbuf.FixedSizeList()).listSize();
            if (nullable) {
              return new NullableFixedSizeListVector(name, size, dataVector);
            } else {
              return new FixedSizeListVector(name, size, dataVector);
            }
         } else if (typeType === Type.Struct_) {
            var vectors : Vector[] = [];
            for (var i : number = 0; i < field.childrenLength(); i += 1|0) {
                vectors.push(vectorFromField(field.children(i), dictionaries));
            }
            return new StructVector(name, vectors);
        } else {
            if (typeType === Type.Int) {
                var type = field.type(new org.apache.arrow.flatbuf.Int());
                return _createIntVector(name, type.bitWidth(), type.isSigned(), nullable)
            } else if (typeType === Type.FloatingPoint) {
                var precision = field.type(new org.apache.arrow.flatbuf.FloatingPoint()).precision();
                if (precision == org.apache.arrow.flatbuf.Precision.SINGLE) {
                    return nullable ? new NullableFloat32Vector(name) : new Float32Vector(name);
                } else if (precision == org.apache.arrow.flatbuf.Precision.DOUBLE) {
                    return nullable ? new NullableFloat64Vector(name) : new Float64Vector(name);
                } else {
                    throw "Unimplemented FloatingPoint precision " + precision;
                }
            } else if (typeType === Type.Utf8) {
                return nullable ? new NullableUtf8Vector(name) : new Utf8Vector(name);
            } else if (typeType === Type.Date) {
                return nullable ? new NullableDateVector(name) : new DateVector(name);
            } else {
                throw "Unimplemented type " + typeType;
            }
        }
    } else {
        // determine arrow type - default is signed 32 bit int
        var type = dictionary.indexType(), bitWidth = 32, signed = true;
        if (type != null) {
            bitWidth = type.bitWidth();
            signed = type.isSigned();
        }
        var indices = _createIntVector(name, bitWidth, signed, nullable);
        return new DictionaryVector(name, indices, dictionaries[dictionary.id().toFloat64().toString()]);
    }
}

function _createIntVector(name, bitWidth, signed, nullable) {
    if (bitWidth == 64) {
        if (signed) {
            return nullable ? new NullableInt64Vector(name) : new Int64Vector(name);
        } else {
            return nullable ? new NullableUint64Vector(name) : new Uint64Vector(name);
        }
    } else if (bitWidth == 32) {
        if (signed) {
            return nullable ? new NullableInt32Vector(name) : new Int32Vector(name);
        } else {
            return nullable ? new NullableUint32Vector(name) : new Uint32Vector(name);
        }
    } else if (bitWidth == 16) {
        if (signed) {
            return nullable ? new NullableInt16Vector(name) : new Int16Vector(name);
        } else {
            return nullable ? new NullableUint16Vector(name) : new Uint16Vector(name);
        }
    } else if (bitWidth == 8) {
        if (signed) {
            return nullable ? new NullableInt8Vector(name) : new Int8Vector(name);
        } else {
            return nullable ? new NullableUint8Vector(name) : new Uint8Vector(name);
        }
    } else {
         throw "Unimplemented Int bit width " + bitWidth;
    }
}
