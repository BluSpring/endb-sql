import { Element, EndbAdapter } from 'endb';
import { EventEmitter } from 'events';
import { SQLDialects } from 'sql-ts/dist/configTypes';
import { TableWithColumns } from 'sql-ts/dist/esm/table';
import { Sql } from 'sql-ts';

abstract class EndbSql<TVal> extends EventEmitter implements EndbAdapter<TVal> {
    namespace: string;
    protected readonly options: Required<EndbSql.EndbSqlOptions>;
    protected readonly db: TableWithColumns<EndbSql.SqlElement<string>>;
    protected readonly query: (sqlString: string) => Promise<any>;

    constructor(options: EndbSql.EndbSqlOptions) {
        super();

        this.options = {
            table: 'endb', keySize: 255, ...options
        };
        const db = new Sql(this.options.dialect);
        this.db = db.define({
            name: this.options.table,
            columns: [
                {
                    name: 'key',
                    primaryKey: true,
                    dataType: `VARCHAR(${Number(this.options.keySize)})`,
                },
                {
                    name: 'value',
                    dataType: 'TEXT',
                },
            ],
        });
        const connected = options
            .connect()
            .then(async (query) => {
            const createTable = this.db.create().ifNotExists().toString();
            await query(createTable);
            return query;
        })
            .catch((error) => {
            this.emit('error', error);
        });
        this.query = async (sqlString) => {
            const query = await connected;
            if (query)
                return query(sqlString);
        };
    }

    async all(): Promise<EndbSql.SqlElement<string>[]> {
        const select = this.db
            .select('*')
            .where(this.db.key.like(`${this.namespace}:%`))
            .toString();
        const rows = await this.query(select);

        return rows;
    }

    async clear(): Promise<void> {
        const del = this.db
            .delete()
            .where(this.db.key.like(`${this.namespace}:%`))
            .toString();

        await this.query(del);
    }

    async delete(key: string): Promise<boolean> {
        const select = this.db.select().where({ key }).toString();
        const del = this.db.delete().where({ key }).toString();
        const [row] = await this.query(select);

        if (row === undefined)
            return false;

        await this.query(del);

        return true;
    }

    async get(key: string): Promise<string | void | TVal> {
        const select = this.db.select().where({ key }).toString();
        const [row] = await this.query(select);
        
        if (row === undefined)
            return undefined;

        return row.value;
    }

    async has(key: string): Promise<boolean> {
        const select = this.db.select().where({ key }).toString();
        const [row] = await this.query(select);

        return Boolean(row);
    }

    async set(key: string, value: string): Promise<any> {
        let upsert: string;

        if (this.options.dialect === 'mysql') {
            value = value.replace(/\\/g, '\\\\');
        }

        if (this.options.dialect === 'postgres') {
            upsert = this.db
                .insert({ key, value })
                .onConflict({
                columns: ['key'],
                update: ['value'],
            }).toString();
        } else {
            upsert = this.db.replace({ key, value }).toString();
        }

        return this.query(upsert);
    }
}

declare namespace EndbSql {
    export interface EndbSqlOptions {
        dialect: SQLDialects;
        connect(): Promise<(sqlString: string) => Promise<unknown>>;
        table?: string;
        keySize?: number;
    }

    // Don't know what this does, but TypeScript
    // absolutely bitches at me if this isn't here
    export interface SqlElement<TElement = string> extends Element<TElement> {
        literal: any;
    }
}

export = EndbSql;