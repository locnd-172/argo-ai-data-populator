import json
import gradio as gr
import time
import threading
from firebase_admin import firestore
import pandas as pd
import firebase_admin
from datetime import datetime

cred = firebase_admin.credentials.Certificate(
    "./smart-agri-bbd45-firebase-adminsdk-jfpri-5e03cc48f1.json"
)
firebase_admin.initialize_app(cred)
db = firestore.client()

df = pd.DataFrame()
timer_value = 0

excel_file = "./data_input.xlsx"
df_excel = pd.read_excel(excel_file)
current_row = 0


row_iterator = df_excel.iterrows()

columns = ["data", "date", "facility", "location", "plant"]
location_data = ["latitude", "longitude", "district", "province", "address"]
weather_data = [
    "max_temperature",
    "min_temperature",
    "wind",
    "wind_direction",
    "humidity",
    "cloud",
    "atmospheric_pressure",
]
soil_data = [
    "soil_moisture",
    "soil_moisture_20",
    "soil_moisture_40",
    "soil_temperature",
    "soil_ph",
    "soil_conductivity",
]
irrigation_data = [
    "water_consumed",
    "water_recycled",
    "irrigation_type",
    "days_flooded",
]
pest_data = ["pest_population_counts", "disease_incidence", "severity_of_infestations"]
energy_data = ["diesel", "gasoline", "electricity"]


def create_data_item(row):
    return {
        "fertilize": row.get("fertilize", None),
        "weather": {key: row.get(key, None) for key in weather_data},
        "pest": {key: row.get(key, None) for key in pest_data},
        "irrigation": {key: row.get(key, None) for key in irrigation_data},
        "soil": {key: row.get(key, None) for key in soil_data},
        "energy": {key: row.get(key, None) for key in energy_data},
    }


def populate_data(stop_event, facility_name):
    global df, current_row, df_excel
    while not stop_event.is_set():
        try:
            _, row = next(row_iterator)
        except StopIteration:
            break

        data_item = create_data_item(row)
        row["datetime"] = datetime.strptime(
            datetime.strptime(
                row["datetime"].strftime("%d/%m/%Y"), "%d/%m/%Y"
            ).strftime("%Y/%m/%d"),
            "%Y/%m/%d",
        )

        # row["facility"] = str(row["facility"]).lower()
        row["facility"] = str(facility_name).lower()
        facility = row.get("facility", None)
        print(f'type: {type(row["datetime"])}')
        data = {
            "data": data_item,
            "date": row["datetime"],
            "facility": str(facility).lower(),
            "location": {key: row.get(key, None) for key in location_data},
            "plant": str(row["plant"]).lower().strip(),
        }
        db.collection("mrv_system").add(data)
        data["date"] = data["date"].strftime("%Y/%m/%d")
        _data = {k: [json.dumps(v, ensure_ascii=False)] for k, v in data.items()}
        print(_data)
        new_row = pd.DataFrame(_data, columns=columns)
        df = pd.concat([df, new_row], ignore_index=True)

        current_row += 1
        time.sleep(5)


def start_populating(facility_name):
    global stop_event, df, current_row, timer_value
    stop_event = threading.Event()
    df = pd.DataFrame(columns=columns)
    current_row = 0
    timer_value = 0

    threading.Thread(target=lambda: populate_data(stop_event, facility_name)).start()
    threading.Thread(target=lambda: update_timer(stop_event)).start()
    return "Populating data..."


def stop_populating():
    global timer_value
    stop_event.set()
    timer_value = 0
    return "Data population stopped."


def update_timer(stop_event):
    global timer_value
    while not stop_event.is_set():
        timer_value += 1
        time.sleep(1)


with gr.Blocks() as demo:
    gr.Markdown("## Sensor Data Populator")

    timer_text = gr.Textbox(label="Time Elapsed (seconds):", value="0")
    with gr.Row():
        facility_name_input = gr.Textbox(label="Facility Name")
        status_text = gr.Textbox(label="Status", value="", interactive=False)
        start_button = gr.Button("Start Populating")
        stop_button = gr.Button("Stop Populating")

    data_table = gr.DataFrame(label="Populated Data", wrap=True)

    start_button.click(
        fn=start_populating, inputs=facility_name_input, outputs=status_text
    )
    stop_button.click(fn=stop_populating, outputs=status_text)

    demo.load(lambda: df, None, outputs=data_table, every=1)
    demo.load(lambda: timer_value, None, outputs=timer_text, every=1)


demo.launch(share=True)
