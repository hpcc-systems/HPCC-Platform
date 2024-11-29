import { BaseRow } from "./Store";
import { Memory, QueryOptions } from "./Memory";

export class MemoryTreeStore<T extends BaseRow = BaseRow> extends Memory<T> {

    protected parentIdProperty: keyof T;

    constructor(idProperty: keyof T, parentIdProperty: keyof T, alphanumSort: { [id: string]: boolean }) {
        super(idProperty, alphanumSort);
        this.parentIdProperty = parentIdProperty;
    }

    mayHaveChildren(item: T): boolean {
        return this.data.some(row => row[this.parentIdProperty] === item[this.responseIDField]);
    }

    getChildren(parent: T, options?: QueryOptions<T>) {
        return this.query({ [this.parentIdProperty]: parent[this.responseIDField] } as any, options);
    }
}
