from flask import Flask, request, render_template, jsonify
import os
from google.cloud import bigquery
import google.generativeai as genai
from dotenv import load_dotenv

app = Flask(__name__)
load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

client = bigquery.Client()

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-pro')

QUERY = """
SELECT id, description FROM `capstone-alterra-424814.dim_tables.dim_complaints` WHERE TIMESTAMP_TRUNC(updated_at, DAY) >= TIMESTAMP("2024-04-17")
"""
Query_Results = client.query(QUERY)
df = Query_Results.to_dataframe()

def get_complaint_by_id(id_complaint, dataset):
    try:
        complaint = dataset.loc[dataset['id'] == id_complaint, 'description'].values[0]
        return complaint
    except IndexError:
        return None

def recommendation(id_complaint, dataset):
    complaint = get_complaint_by_id(id_complaint, dataset)
    if not complaint:
        return "Complaint ID not found in the dataset.", None

    prompt = f"""
                Anda adalah seorang admin yang bertugas mengelola keluhan dan komplain di masyarakat wilayah provinsi di Indonesia, Suatu hari ada komplain yang masuk seperti berikut
                {complaint}
                berdasarkan komplain tersebut apa yang akan anda jawab kepada pengadu tersebut?
            """
    analysis_result = model.generate_content(prompt)
    response_text = analysis_result.text
    max_words_per_line = 10
    words = response_text.split()
    formatted_result = '\n'.join(
        ' '.join(words[i:i + max_words_per_line]) for i in range(0, len(words), max_words_per_line)
    )

    return formatted_result, complaint

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        id_complaint = request.form['id_complaint']
        hasil, complaint = recommendation(id_complaint, df)
        if hasil:
            return render_template('index.html', id_complaint=id_complaint, complaint=complaint)
        else:
            return render_template('index.html', hasil="Complaint ID not found in the dataset.", id_complaint=id_complaint)
    return render_template('index.html', hasil=None)

@app.route('/get_recommendation', methods=['POST'])
def get_recommendation():
    id_complaint = request.form['id_complaint']
    hasil, complaint = recommendation(id_complaint, df)
    return jsonify({'hasil': hasil, 'complaint': complaint})

