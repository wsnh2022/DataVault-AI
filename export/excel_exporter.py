"""
outputs/excel_exporter.py - Exports a query result DataFrame to Excel (.xlsx).

Uses openpyxl engine via pandas. Returns bytes for st.download_button.
Applies minimal formatting: bold header, auto column width, frozen top row.
"""

import io
import pandas as pd


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Results") -> bytes:
    """
    Converts a DataFrame to an Excel workbook and returns raw bytes.
    Caller passes the bytes to st.download_button(data=...).

    Formatting:
    - Bold header row
    - Column widths auto-sized to content (capped at 50 chars)
    - Top row frozen for scrollability
    """
    buffer = io.BytesIO()

    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)

        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        # Bold the header row
        from openpyxl.styles import Font, PatternFill, Alignment
        header_font = Font(bold=True)
        header_fill = PatternFill(
            start_color="D3D3D3", end_color="D3D3D3", fill_type="solid"
        )
        for cell in worksheet[1]:                       # row 1 = header
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal="center")

        # Freeze top row so headers stay visible when scrolling
        worksheet.freeze_panes = "A2"

        # Auto-size column widths based on content
        for col_cells in worksheet.columns:
            max_len = 0
            col_letter = col_cells[0].column_letter
            for cell in col_cells:
                try:
                    val_len = len(str(cell.value)) if cell.value is not None else 0
                    max_len = max(max_len, val_len)
                except Exception:
                    pass
            worksheet.column_dimensions[col_letter].width = min(max_len + 2, 50)

    return buffer.getvalue()
