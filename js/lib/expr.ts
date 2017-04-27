import * as types from './types';

export interface Expr {
    eval(idx: number, cols: Array<Array<any>>);
    bind(schema: types.Vector);
    getChildren(): Array<Expr>;
    emitString();
};

abstract class AbstractExpr implements Expr {
    abstract eval(idx: number, cols: Array<Array<any>>);
    abstract emitString();
    bind(schema) {
        this.getChildren().forEach(c => c.bind(schema));
    };
    getChildren() { return []; }
};

export class And extends AbstractExpr {
    constructor(public left: Expr, public right: Expr) {
        super();
    }

    bind(schema: types.Vector) {
        this.left.bind(schema);
        this.right.bind(schema);
    };

    eval(idx: number, cols: Array<Array<any>>) {
        return this.left.eval(idx,cols) && this.right.eval(idx,cols);
    };
    emitString() {
        return `${this.left.emitString()} && ${this.right.emitString()}`;
    };

    getChildren() { return [this.left, this.right]; }
};

export class Or extends AbstractExpr {
    constructor(public left: Expr, public right: Expr) {
        super();
    }

    eval(idx: number, cols: Array<Array<any>>) {
        return this.left.eval(idx, cols) && this.right.eval(idx, cols);
    };
    emitString() {
        return `${this.left.emitString()} || ${this.right.emitString()}`;
    };

    getChildren() { return [this.left, this.right]; }
};

export class Equals extends AbstractExpr {
    constructor(public left: Expr, public right: Expr) {
        super();
    }

    eval(idx: number, cols: Array<Array<any>>) {
        return this.left.eval(idx, cols) == this.right.eval(idx, cols);
    };
    emitString() {
        return `${this.left.emitString()} == ${this.right.emitString()}`;
    }

    getChildren() { return [this.left, this.right]; }
};

export class Literal extends AbstractExpr {
    constructor(public v: number) {
        super();
    }

    eval(idx: number, cols: Array<Array<any>>) { return this.v; }
    emitString() { return `${this.v}`; }
};

export class Col extends AbstractExpr {
    colidx: number;

    constructor(public name: string) {
        super();
    }

    bind(schema: types.Vector) {
        this.colidx = schema.getChildVectors().findIndex(v => v.name.indexOf(this.name) != -1);
    }

    eval(idx: number, cols: Array<Array<any>>) {
        return cols[this.colidx][idx];
    }

    emitString() { return `${this.name}`; }
};

export class UnaryFunc extends AbstractExpr {
    constructor(public f: (any) => any, public child: Expr) {
        super();
    };

    eval(idx: number, cols: Array<Array<any>>) {
        return this.f(this.child.eval(idx, cols));
    };

    getChildren() { return [this.child] };

    emitString() { return ""; }
};

export function eq(l: Expr, r: Expr): Expr {
    return new Equals(l, r);
};

export function lit(n: number): Expr {
    return new Literal(n);
};

export function col(n: string): Expr {
    return new Col(n);
};

export function and(l: Expr, r: Expr): Expr {
    return new And(l,r);
};

export function or(l: Expr, r: Expr): Expr {
    return new Or(l,r);
};

export function unaryfunc(f: (any) => any, child: Expr): Expr {
    return new UnaryFunc(f, child);
};
