from flask import Flask, render_template, jsonify
import subprocess
from influxdb import InfluxDBClient

app = Flask(__name__)
client = InfluxDBClient(host='localhost', port=8086)
client.switch_database('air_quality')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/air_quality')
def air_quality():
    results = client.query('SELECT * FROM air_quality')
    points = list(results.get_points())
    return jsonify(points)

@app.route('/api/predict')
def predict():
    result = subprocess.run(['spark-submit', 'machine_learning/air_quality_model.py'], capture_output=True, text=True)
    return jsonify({"result": result.stdout})

if __name__ == '__main__':
    app.run(debug=True)
