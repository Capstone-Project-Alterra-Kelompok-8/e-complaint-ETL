from flask import Flask, request, render_template
import os
from google.cloud import bigquery
from openai import OpenAI
import pandas as pd
from dotenv import load_dotenv

app = Flask(__name__)
load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
client = bigquery.Client()

QUERY = """
SELECT id, description FROM `capstone-alterra-424814.dim_tables.dim_complaints` WHERE TIMESTAMP_TRUNC(updated_at, DAY) >= TIMESTAMP("2024-04-17")
"""
Query_Results = client.query(QUERY)
df = Query_Results.to_dataframe()

client = OpenAI(
    api_key = os.getenv("OPENAI_API_KEY"),
)

def generate(prompt, model="gpt-3.5-turbo", max_tokens=2048):
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "user",
                "content": prompt,
            },
        ],
        max_tokens=max_tokens
    )
    return response.choices[0].message.content

def get_complaint_by_id(id_complaint, dataset):
    try:
        complaint = dataset.loc[dataset['id'] == id_complaint, 'description'].values[0]
        return complaint
    except IndexError:
        return None

def recommendation(id_complaint, dataset):
    complaint = get_complaint_by_id(id_complaint, dataset)
    if not complaint:
        return "Complaint ID not found in the dataset."
    
    prompt = f"""
                Anda adalah seorang admin yang bertugas mengelola keluhan dan komplain di masyarakat wilayah provinsi di Indonesia, Suatu hari ada komplain yang masuk seperti berikut
                {complaint}
                berdasarkan komplain tersebut apa yang akan anda jawab kepada pengadu tersebut?
            """
    analysis_result = generate(prompt)
    max_words_per_line = 10
    words = analysis_result.split()
    formatted_result = '\n'.join(
        ' '.join(words[i:i + max_words_per_line]) for i in range(0, len(words), max_words_per_line)
    )
    
    return formatted_result

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        id_complaint = request.form['id_complaint']
        hasil = recommendation(id_complaint, df)
        return render_template('index.html', hasil=hasil, id_complaint=id_complaint)
    return render_template('index.html', hasil=None)

if __name__ == '__main__':
    app.run(debug=True)
