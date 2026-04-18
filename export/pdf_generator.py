"""
outputs/pdf_generator.py - Exports a query result DataFrame to PDF.

Uses fpdf2 (FPDF2 library). No external LaTeX, no heavy dependencies.
Returns bytes so Streamlit's st.download_button can consume it directly.
"""

import io
import pandas as pd
from fpdf import FPDF
from fpdf.enums import XPos, YPos


def dataframe_to_pdf_bytes(df: pd.DataFrame, title: str = "Query Results") -> bytes:
    """
    Converts a DataFrame to a PDF table and returns raw bytes.
    Caller passes the bytes to st.download_button(data=...).

    Layout decisions:
    - Landscape orientation for wide tables
    - Column width auto-calculated from page width / column count
    - Header row uses bold + light grey fill
    - Body rows alternate white / very light grey for readability
    """
    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=10)
    pdf.add_page()

    # Title
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, title, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
    pdf.ln(3)

    if df.empty:
        pdf.set_font("Helvetica", "", 11)
        pdf.cell(0, 10, "No data to display.", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        return bytes(pdf.output())

    columns = df.columns.tolist()
    page_width = pdf.w - 2 * pdf.l_margin     # usable width in mm
    col_w = page_width / len(columns)           # equal-width columns

    # Header row
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_fill_color(220, 220, 220)           # light grey
    for col in columns:
        label = str(col)[:20]                   # truncate very long names
        pdf.cell(col_w, 7, label, border=1, fill=True, align="C")
    pdf.ln()

    # Data rows
    pdf.set_font("Helvetica", "", 8)
    for i, row in df.iterrows():
        fill = (i % 2 == 0)                     # alternate row shading
        pdf.set_fill_color(245, 245, 245) if fill else pdf.set_fill_color(255, 255, 255)
        for col in columns:
            cell_val = str(row[col]) if pd.notna(row[col]) else ""
            cell_val = cell_val[:30]             # truncate long cell values
            pdf.cell(col_w, 6, cell_val, border=1, fill=fill)
        pdf.ln()

    return bytes(pdf.output())
