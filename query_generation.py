from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline
import psycopg2

# Step 1: Load the Pretrained Model and Tokenizer
model_name = 'bugdaryan/Code--2-13B-instruct-text2sql'
model = AutoModelForCausalLM.from_pretrained(model_name, device_map='auto')
tokenizer = AutoTokenizer.from_pretrained(model_name)
pipe = pipeline('text-generation', model=model, tokenizer=tokenizer)


def fetch_schema(database_url):
    """
    Fetch the schema details from the PostgreSQL database.
    """
    schema = ""
    try:
        # Establish connection
        connection = psycopg2.connect(database_url)
        cursor = connection.cursor()
        
        # Query to fetch all table names
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        tables = cursor.fetchall()

        # Fetch column details for each table
        for table in tables:
            table_name = table[0]
            schema += f"TABLE {table_name} ("

            cursor.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}';
            """)
            columns = cursor.fetchall()
            schema += ", ".join([f"{col[0]} {col[1]}" for col in columns])
            schema += "); "
        
    except Exception as e:
        print("Error fetching schema:", e)
    finally:
        if connection:
            cursor.close()
            connection.close()
    
    return schema

database_url = "postgresql://postgres:55555@localhost:5432/knitting"
table_schema = fetch_schema(database_url)
question = 'how many rows in the table defect details'

# Step 5: Create the Prompt Dynamically
prompt = f"[INST] Write SQL query to answer the following question given the database schema. Please wrap your code answer using ```: Schema: {table_schema} Question: {question} [/INST] Here is the SQL query to answer the question: {question}: ``` "

# Step 6: Generate the SQL Query
ans = pipe(prompt, max_new_tokens=100)
print("Generated SQL Query:")
print(ans[0]['generated_text'].split('```')[2])