
import base64
from dbconnection import get_connection, return_connection

# Export face encodings as SQL insert statements
def export_face_encodings_sql(filename='face_encodings.sql'):
    conn = get_connection()
    cur = conn.cursor()
    
    export_query = "SELECT id, name, encoding FROM tbl_known_faces"
    
    with open(filename, mode='w') as file:
        file.write("-- Insert face encodings into tbl_known_faces\n")
        cur.execute(export_query)
        
        for row in cur.fetchall():
            id, name, encoding = row
            encoding_base64 = base64.b64encode(encoding).decode('utf-8')
            insert_statement = (
                f"INSERT INTO public.tbl_known_faces (id, name, encoding) "
                f"VALUES ({id}, '{name}', DECODE('{encoding_base64}', 'base64')) "
                f"ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, encoding=EXCLUDED.encoding;\n"
            )
            file.write(insert_statement)
    
    cur.close()
    return_connection(conn)
    print(f"Face encodings exported to {filename}")

if __name__ == '__main__':
    export_face_encodings_sql()