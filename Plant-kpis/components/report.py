import plotly.io as pio
import io
import base64
import pdfkit
from datetime import datetime
path_to_wkhtmltopdf = 'C:/Program Files/wkhtmltopdf/bin/wkhtmltopdf.exe'
config = pdfkit.configuration(wkhtmltopdf=path_to_wkhtmltopdf)

def generate_pdf_report(titles, figures):
    logo_path = "app/assets/image-removebg-preview 1.png"
    titles.pop(3) 
    for fig in figures:
        print(type(fig))
    for title in titles:
        print(title)
    today_date = datetime.today().strftime('%Y-%m-%d')

    # Encode the company logo in base64
    with open(logo_path, "rb") as image_file:
        encoded_logo = base64.b64encode(image_file.read()).decode('utf-8')

   
    
    # Define the basic structure of the HTML report
    report_content = f"""
    <html>
    <head>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
        }}
        h1 {{
            text-align: center;
            font-size: 24px;
        }}
        h2 {{
            font-size: 20px;
            margin-top: 20px;
            text-align: center;
        }}
        .figure-container {{
            width: 100%;
            clear: both;
        }}
        .figure {{
            width: 48%;
            float: left;
            margin-bottom: 20px;
        }}
        .figure img {{
            max-width: 100%;
            height: auto;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 5px;
        }}
        .header {{
            text-align: center;
            margin-bottom: 40px;
        }}
        .header img {{
            width: 150px;
            height: auto;
        }}
        .header .date {{
            font-size: 16px;
            margin-top: 5px;
        }}
        .clearfix {{
            clear: both;
        }}
    </style>
    </head>
    <body>
    <div class="header">
        <img src="data:image/png;base64,{encoded_logo}" alt="Company Logo" />
        <div class="date">Date: {today_date}</div>
    </div>
    <h1>Inventory Report</h1>
    """

    # Add each figure to the report, ensuring two figures per row
    for i in range(0, len(titles), 2):
        report_content += "<div class='figure-container'>"
        
        # First figure
        buffer = io.BytesIO()
        pio.write_image(figures[i], buffer, format='png', width=600, height=400)
        buffer.seek(0)
        encoded_image = base64.b64encode(buffer.read()).decode('utf-8')
        report_content += f'<div class="figure"><h2>{titles[i]}</h2><img src="data:image/png;base64,{encoded_image}" /></div>'

        # Second figure, if it exists
        if i + 1 < len(titles):
            buffer = io.BytesIO()
            pio.write_image(figures[i + 1], buffer, format='png', width=600, height=400)
            buffer.seek(0)
            encoded_image = base64.b64encode(buffer.read()).decode('utf-8')
            report_content += f'<div class="figure"><h2>{titles[i + 1]}</h2><img src="data:image/png;base64,{encoded_image}" /></div>'

        report_content += "</div>"  # Close figure-container

    report_content += """
    </body>
    </html>
    """

    options = {
        'page-size': 'A4',
        'margin-top': '10mm',
        'margin-right': '10mm',
        'margin-bottom': '10mm',
        'margin-left': '10mm',
        'encoding': "UTF-8",
        'no-outline': None,
    }

    pdf = pdfkit.from_string(report_content, False, configuration=config, options=options)

    return pdf