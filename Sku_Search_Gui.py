import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import sqlite3
import os
import hashlib
import gspread
from google.oauth2.service_account import Credentials

# ---------- Google Sheets Functions ----------
def connect_to_google_sheet(json_key_path, sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(json_key_path, scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open(sheet_name).sheet1
    return sheet

# ---------- SQLite Functions ----------
def get_table_name_from_path(csv_path):
    return os.path.splitext(os.path.basename(csv_path))[0]

def hash_dataframe(df):
    return hashlib.md5(pd.util.hash_pandas_object(df, index=True).values).hexdigest()

def csv_to_sqlite_if_updated(csv_file_path, sqlite_db='data.db'):
    try:
        new_df = pd.read_csv(csv_file_path)
        table_name = get_table_name_from_path(csv_file_path)

        conn = sqlite3.connect(sqlite_db)
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        exists = cursor.fetchone()

        if exists:
            old_df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            if hash_dataframe(new_df) == hash_dataframe(old_df):
                conn.close()
                return table_name

        new_df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        return table_name

    except Exception as e:
        messagebox.showerror("Error", str(e))
        return None

def find_sku_column(columns):
    for col in columns:
        if col.strip().lower() == "sku":
            return col
    return None

def search_skus_and_write_to_sheet(db_path, table_name, sku_column, sku_list, sheet):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    placeholders = ','.join(['?'] * len(sku_list))
    query = f"SELECT * FROM {table_name} WHERE {sku_column} IN ({placeholders})"
    cursor.execute(query, sku_list)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)

    # Clear the sheet before writing
    sheet.clear()
    sheet.append_row(columns)

    sku_col_actual = find_sku_column(df.columns)
    found_skus = set(df[sku_col_actual].astype(str).str.strip().tolist())
    input_skus_set = set([sku.strip() for sku in sku_list])
    not_found = list(input_skus_set - found_skus)

    for row in df.values.tolist():
        sheet.append_row(row)

    for missing in not_found:
        sheet.append_row([f"❌ SKU not found: {missing}"])

    conn.close()

# ---------- GUI ----------
def run_gui():
    def browse_file():
        file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        csv_path.set(file_path)

    def execute():
        path = csv_path.get()
        json_path = json_key_path.get()
        sheet_name = sheet_entry.get()
        sku_values = sku_input.get().split(',')

        if not all([path, json_path, sheet_name, sku_values]):
            messagebox.showwarning("Missing Info", "Please fill in all fields")
            return

        db_name = "data_gui.db"
        table = csv_to_sqlite_if_updated(path, db_name)

        if table:
            try:
                sheet = connect_to_google_sheet(json_path, sheet_name)
                search_skus_and_write_to_sheet(db_name, table, "sku", sku_values, sheet)
                messagebox.showinfo("Done", "Search completed and data sent to Google Sheets.")
            except Exception as e:
                messagebox.showerror("Google Sheets Error", str(e))

    window = tk.Tk()
    window.title("SKU Search Tool")
    window.geometry("500x300")

    tk.Label(window, text="CSV File Path").pack()
    csv_path = tk.StringVar()
    tk.Entry(window, textvariable=csv_path, width=60).pack()
    tk.Button(window, text="Browse", command=browse_file).pack()

    tk.Label(window, text="Google Service JSON Path").pack()
    json_key_path = tk.StringVar()
    tk.Entry(window, textvariable=json_key_path, width=60).pack()

    tk.Label(window, text="Google Sheet Name").pack()
    sheet_entry = tk.Entry(window, width=60)
    sheet_entry.pack()

    tk.Label(window, text="Enter SKUs (comma-separated)").pack()
    sku_input = tk.Entry(window, width=60)
    sku_input.pack()

    tk.Button(window, text="Run", command=execute).pack(pady=10)
    window.mainloop()

if __name__ == "__main__":
    run_gui()
