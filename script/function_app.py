import azure.functions as func
import os
import logging
import base64
import datetime
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import pandas as pd
from tabulate import tabulate

import sendgrid
from sendgrid.helpers.mail import Mail, Email, To, Content, Attachment, FileContent, FileName, FileType, Disposition, ContentId

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="recommender/warm-up-cache-images/{name}.csv",
                               connection="strecommenderpddev001_STORAGE") 
@app.blob_output(arg_name="outblob", path="logs/{DateTime}-{name}.txt",
                 connection="AzureWebJobsStorage")
@app.blob_output(arg_name="outblobresults", path="logs/{DateTime}-{name}-results.csv",
                 connection="AzureWebJobsStorage")

def main(myblob: func.InputStream , outblob: func.Out[str], outblobresults: func.Out[str]) : 
    global results    
    
    sendgrid_api_key = os.environ["SENDGRID_API_KEY"]

    # create email object with defined parameters
    sender_email = os.environ["SENDER_EMAIL"]
    dst_email = os.environ["EMAIL"]
    email_subject = "Folheto Personalizado | CDN Warm Up results"
    
    results = pd.DataFrame(columns=['url','http_code', 'time', 'age', 'x-cache'])
    logging.info(f"Python blob trigger function processed blob"
                 f"Name: {myblob.name}")
    fileData = pd.read_csv(myblob)
    links = fileData['IMAGE_LINK']
    if len(links) > 0:
        for url in links:
            logging.info(f"Getting {url}")
            connection_started_time = None
            connection_made_time = None
            connection_started_time = datetime.datetime.now()
            time_delta = 0
            response_status = 0
            age = 0
            try:
                req = Request(str(url))
                req.add_header('User-Agent','CDNWarmup/1.0')
                resp = urlopen(req,timeout=2)
                connection_made_time = datetime.datetime.now()
                time_delta = connection_made_time - connection_started_time
                response_status = resp.status
                age = resp.getheader('age')
                cache_hit = resp.getheader('x-cache')
                del req
            except HTTPError as error:
                response_status = error.code  # 404, 500, etc
                cache_hit = 'Got an error from the site'
            except Exception as e:
                response_status = 0
                cache_hit = 'Something is wrong with URL'
            res = { 'url' : url,
                    'http_code': response_status,
                    'time': time_delta,
                    'age' : age ,
                    'x-cache' : cache_hit
                    }
            results.loc[len(results)] = res
        
        email_content = tabulate(results.groupby(['http_code']).size().to_frame(), ['HTTP Code','Count'], tablefmt="simple") + '\n\n\n' + tabulate(results.groupby(['x-cache']).size().to_frame(), ['Cache Hit', 'Count'], tablefmt="simple") + '\n'
        outblob.set(email_content)
        outblobresults.set(results.to_csv(encoding='utf-8', index=False))

        # lets send the email with the results also
        mail = Mail(
            from_email=Email(sender_email),
            to_emails=  [ To(email) for email in dst_email.split(",") ],
            subject=email_subject,
            plain_text_content=email_content)

        try:
            encoded = base64.b64encode(results.to_csv(encoding='utf-8', index=False).encode()).decode()
            attachment = Attachment()
            attachment.file_content = FileContent(encoded)
            attachment.file_name = FileName("cdn-warmup-results.csv")
            attachment.disposition = Disposition('attachment')
            attachment.content_id = ContentId('Content ID')
            mail.attachment = attachment
            print('attachment object created')
       
        except Exception as e:
            print(f'failed to create attachment message: {str(e)}')

        # send email
        try:
            sg = sendgrid.SendGridAPIClient(api_key=sendgrid_api_key)
            response = sg.client.mail.send.post(request_body=mail.get())
            logging.info(response)
        except Exception as e:
            logging.error(f'failed to call sendgrid api - response: {response.status_code} - message: {str(e)}')
                
        del links
        results = results.iloc[0:0]