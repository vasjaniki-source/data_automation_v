import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime

class HistoryPanelWidget(ttk.Frame):
    def __init__(self, parent, pipeline):
        super().__init__(parent, padding="10")
        self.pipeline = pipeline
        self.create_widgets()

    def create_widgets(self):
        cols = ("time", "op", "status", "details")
        self.tree = ttk.Treeview(self, columns=cols, show="headings")
        
        self.tree.heading("time", text="Время")
        self.tree.heading("op", text="Операция")
        self.tree.heading("status", text="Статус")
        self.tree.heading("details", text="Детали")
        
        self.tree.column("time", width=120)
        self.tree.column("op", width=150)
        self.tree.column("status", width=100)
        self.tree.column("details", width=400)
        
        self.tree.pack(fill="both", expand=True)

        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill="x", pady=10)
        
        ttk.Button(btn_frame, text="Очистить историю", command=self.clear_history).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Экспорт в CSV", command=self.export_history).pack(side="left", padx=5)

    def add_entry(self, operation, status, details):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.tree.insert("", "end", values=(timestamp, operation, status, details))

    def clear_history(self):
        for item in self.tree.get_children():
            self.tree.delete(item)

    def export_history(self):
        path = filedialog.asksaveasfilename(defaultextension=".csv")
        if path:
            import csv
            with open(path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Время", "Операция", "Статус", "Детали"])
                for item in self.tree.get_children():
                    writer.writerow(self.tree.item(item)['values'])
            messagebox.showinfo("Экспорт", "История экспортирована")