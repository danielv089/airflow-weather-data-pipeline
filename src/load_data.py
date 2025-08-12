import io

def copy_df_to_table(df, table_name, conn):


    
    with conn.cursor() as cursor:
        buffer=io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        with cursor.copy(f"COPY {table_name} FROM STDIN WITH CSV HEADER") as copy:
            for line in buffer:
                copy.write(line)
        conn.commit()



