import gradio as gr
from lstore.db import Database
from lstore.query import Query

# Initialize your database and query object
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)


# Define wrapper functions for your database operations
def insert_record(student_id, grade1, grade2, grade3, grade4):
    if query.insert(student_id, grade1, grade2, grade3, grade4):
        return "Insertion Complete"
    else:
        return "RID already exist"


def select_record(student_id):
    result = query.select(student_id, 0, [1, 1, 1, 1, 1])
    if result:
        return str(result[0].columns)  # Assuming the first record is what you want
    return "No record found."


def update_record(student_id, grade1, grade2, grade3, grade4):
    if query.update(student_id, *([student_id, grade1, grade2, grade3, grade4])):
        return "Update is complete"
    else:
        return "Update encounter with an error, check RID"


def delete_record(student_id):
    result = query.delete(student_id)
    if result:
        return f"Record with key: {student_id} successfully deleted."
    return f"Delete error on key: {student_id}"


def sum_record(start_range, end_range, aggregate_column_index):
    result = query.sum(int(start_range), int(end_range), int(aggregate_column_index))
    if result is not False:
        return str(result)
    return "Sum operation failed or no records in range."


def display_data():
    records = query.traverse_table()
    if not records or records == [[]]:
        return "<div>No data available.</div>"

    # Start the HTML table
    html_output = ("<div><strong>Table: Grades</strong><br><table border='1'><tr><th>Key</th><th>Student "
                   "ID</th><th>Grade1</th><th>Grade2</th><th>Grade3</th></tr>")

    # Add rows for each record
    for record in records:
        html_output += "<tr>" + "".join(f"<td>{col}</td>" for col in record) + "</tr>"

    # Close the table
    html_output += "</table></div>"

    return html_output


with gr.Blocks() as gui:
    gr.Markdown("## Database Operations GUI")

    with gr.Row():
        with gr.Column(scale=1):
            gr.Markdown("### Write Type Functions")
            with gr.Row():
                gr.Button("Insert").click(insert_record,
                                          inputs=[gr.Number(label="Key"), gr.Number(label="Student ID"),
                                                  gr.Number(label="Grade 1"), gr.Number(label="Grade 2"),
                                                  gr.Number(label="Grade 3")], outputs=gr.Textbox(label="Status"))
                gr.Button("Update").click(update_record,
                                          inputs=[gr.Number(label="Key"), gr.Number(label="Student ID"),
                                                  gr.Number(label="Grade 1"), gr.Number(label="Grade 2"),
                                                  gr.Number(label="Grade 3")], outputs=gr.Textbox(label="Status"))
                gr.Button("Delete").click(delete_record, inputs=[gr.Number(label="Key")],
                                          outputs=gr.Textbox(label="Status"))
        with gr.Column(scale=1):
            gr.Markdown("### Read Type Functions")
            gr.Button("Select").click(select_record, inputs=[gr.Number(label="Key")],
                                      outputs=gr.Textbox(label="Result"))
            gr.Button("Sum").click(sum_record, inputs=[gr.Number(label="Start Range"), gr.Number(label="End Range"),
                                                       gr.Number(label="Column Index")],
                                   outputs=gr.Textbox(label="Sum Result"))

    gr.Markdown("## Current Data Storage")
    data_display_html = gr.HTML(label="Data Display")
    gr.Button("Refresh Data").click(display_data, inputs=[], outputs=data_display_html)

gui.launch(share=True)
