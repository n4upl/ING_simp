#!/usr/bin/env python3

import sys
import logging
logging.basicConfig(level=logging.INFO)
import requests
import textwrap
import datetime
import argparse
#import lxml
from lxml import etree as ET
from lxml import objectify

import smtplib
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders


# constants
wsdl_url='https://ws.ingbusiness.pl/ing-ccs/cdc00101?wsdl'
cert_file='554168clean.pem'
headers = {"content-type" : "application/soap+xml"}


def remove_namespace(doc):
        """Remove namespace in the passed document in place."""
        for elem in doc.getiterator():
            if type(elem) is ET._Element:
                if elem.tag.startswith('{'):
                    ns_length = elem.tag.find('}') + 1
                    elem.tag = elem.tag[ns_length:]
        objectify.deannotate(doc, cleanup_namespaces=True)
        return doc


def send_mail(send_from, send_to, subject, message, files=[], server="localhost", port=587, username='', password='', use_tls=True, use_auth=True):
    """Compose and send email with provided info and attachments.

    Args:
        send_from (str): from name
        send_to (list[str]): to name(s)
        subject (str): message title
        message (str): message body
        files (list[str]): list of file paths to be attached to email
        server (str): mail server host name
        port (int): port number
        username (str): server auth username
        password (str): server auth password
        use_tls (bool): use TLS mode
    """
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(message))

    for path in files:
        part = MIMEBase('application', "octet-stream")
        with open(path, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename={}'.format(Path(path).name))
        msg.attach(part)

    smtp = smtplib.SMTP(server, port)
    if use_tls:
        smtp.starttls()
    if use_auth:
        smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()

class SimpReportException(Exception):
    pass


class SimpReport:
    """ ING SIMP report handler """

    def __init__(self, args):
        self.args = args
        self.simp_report_date = args.date
        self.simp_code = args.simpcode

    def get_soap_request_body(self, msg_id, rpt_id, date):
        soap_request_body_template = """
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:ca:std:ccs:ing:tech:xsd:mhdr.001.001.01" xmlns:urn1="urn:ca:std:cdc:tech:xsd:ing.cdc.001.01" xmlns:urn2="urn:ca:std:ccs:ing:tech:xsd:rpts.013.001.01">
               <soapenv:Header/>
               <soapenv:Body>
                  <urn1:GetMCReport>
                     <urn2:Document>
                        <urn2:GetMCRpt>
                           <urn2:MsgId>
                              <urn2:Id>{msg_id}</urn2:Id>
                           </urn2:MsgId>
                           <urn2:RptMCQryDef>
                              <urn2:RptMCCrit>
                                 <urn2:NewCrit>
                                    <urn2:SchCrit>
                                       <urn2:Cdtr>
                                          <urn2:EQ>{simp_code}</urn2:EQ>
                                       </urn2:Cdtr>
                                       <urn2:RptDt>
                                          <urn2:DtSch>
                                             <urn2:Dt>{date}</urn2:Dt>
                                          </urn2:DtSch>
                                       </urn2:RptDt>
                                       <urn2:RptId>
                                          <urn2:EQ>{rpt_id}</urn2:EQ>
                                       </urn2:RptId>
                                    </urn2:SchCrit>
                                 </urn2:NewCrit>
                              </urn2:RptMCCrit>
                           </urn2:RptMCQryDef>
                        </urn2:GetMCRpt>
                     </urn2:Document>
                  </urn1:GetMCReport>
               </soapenv:Body>
            </soapenv:Envelope>
        """
    
        return textwrap.dedent(soap_request_body_template).format(msg_id=msg_id, date=date, rpt_id=rpt_id, simp_code=self.simp_code)

    def send_soap_request(self, msg_id, rpt_id, date=None):
        if date is None:
            date = self.simp_report_date
        soap_request = self.get_soap_request_body(msg_id, rpt_id, date)
        r = requests.post(url=wsdl_url, cert=cert_file, data=soap_request, headers=headers)
        soap_response = remove_namespace(ET.ElementTree(ET.fromstring(r.text)))
        if self.args.verbose:
            print(f"SOAP XML REQEST: {soap_request}")
            print(f"SOAP XML RESPONSE: {ET.tostring(soap_response)}")

        err = soap_response.xpath('//RuleDesc/text()')
        if len(err):
            logging.error(f"Error processing soap request: {err}")
            logging.debug(f"SOAP request: {soap_request}")
            logging.debug(f"SOAP response: {ET.tostring(soap_response)}")
            #if not self.args.force:
            #    #raise SimpReportException(f"{err}")
            #    logging.error(f"There are no simp reports for: {self.simp_report_date}. {err}")

        return soap_response

    def get_rpt_id(self, date=None, rpt_id=0):
        if date is None:
            date = self.simp_report_date
        msg_id = datetime.datetime.now().strftime('%s')
        xml_response = self.send_soap_request(msg_id, rpt_id, date)
        rpt_id = xml_response.xpath('//GetMCReportResponse/Document/MCRpt/Rpt/RptId/EQ/text()')[0]

        return int(rpt_id)

    def get_single_report(self, date=None, rpt_id=0):
        if date is None:
            date = self.simp_report_date

        data = {
            'simp_report': None,
            'date': date,
            'n_records': 0,
            'rpt_id': 0,
        }
        data['rpt_id'] = self.get_rpt_id(data['date'], rpt_id) + 1
        msg_id = datetime.datetime.now().strftime('%s')
        xml_response = self.send_soap_request(msg_id, data['rpt_id'], data['date'])
        ntrys = xml_response.xpath('//Ntry')
        logging.debug("number of Ntry records:", len(ntrys))
        data['n_records'] = len(ntrys)
        if len(ntrys) > 0:
            data['simp_report'] = self.ntrys2simp(ntrys)
        else:
            logging.info(f"There are no simp reports for date: {data['date']} rpt_id: {data['rpt_id']}")

        return data
    
    def get_reports(self, date=None):
        """
            Gets all SIMP reports for a given day.
            TODO: implement option to get more than one report per day.
        """

        report = self.get_single_report(date, 0)
        #report2 = self.get_single_report(date, report['rpt_id'])
        
        return report
        

    def ntrys2simp(self, ntrys=[]):
        """
            converts series of <Ntry/> xml elements to simp raport format

        """

        data = {}
        data['tr_count'] = 0
        data['tr_total'] = 0
        lines = ''
       
        for ntry in ntrys:
            if self.args.verbose:
                print(ET.tostring(ntry))
            data['tr_count'] += 1
            data['booking_date'] = ntry.xpath('./BookgDt/Dt/text()')[0]
            data['tr_date'] = ntry.xpath('./TxDt/text()')[0]
            data['tr_src'] = ntry.xpath('./TrnSrc/text()')[0]
            data['sgn'] = ntry.xpath('./OpSgn/text()')[0]
            data['acccount_id'] = ntry.xpath('./SimpAcct/Id/text()')[0]
            data['currency'] = ntry.xpath('./SimpAcct/Ccy/text()')[0]
            data['payer_id'] = ntry.xpath('./Dbtr/Id/text()')[0]
            try:
                data['payer_name1'] = ntry.xpath('./Dbtr/Nm')[0].text or ""
            except IndexError:
                data['payer_name1'] = ""
            try:
                data['payer_name2'] = ntry.xpath('./Dbtr/Nm')[1].text or ""
            except IndexError:
                data['payer_name2'] = ""
            try:
                data['payer_name3'] = ntry.xpath('./Dbtr/Nm')[2].text or ""
            except IndexError:
                data['payer_name3'] = ""
            try:
                data['payer_name4'] = ntry.xpath('./Dbtr/Nm')[3].text or ""
            except IndexError:
                data['payer_name4'] = ""
            data['memo'] = ntry.xpath('./MemoFld/MemoFldLn/text()')[0]
            data['ammount'] = ntry.xpath('./AmtDtls/text()')[0]
            data['cammount'] = int(float(data['ammount']) * 100)
            data['tr_total'] += float(data['ammount'])

            line = f"""{data['acccount_id']},{data['cammount']},{data['sgn']},{data['currency']},{data['booking_date']},UZN,,,"{data['payer_id']}","{data['payer_name1']}","{data['payer_name2']}","{data['payer_name3']}","{data['payer_name4']}","{data['memo']}","","","",{data['tr_src']},,{data['tr_date']}\n"""
            lines += line
 
        #print(data)
        template_header = f"""<SIMP2>666,{data['booking_date']}\n"""
        template_footer = f"""</SIMP2>"il.trn.:{data['tr_count']} wart.trn.:{data['tr_total']}"\n"""

        ret = template_header + lines + template_footer
        
        return ret

    def get(self, mode):
        report = self.get_reports(self.args.date)

        if report['simp_report'] is not None:
            print(report['simp_report'])

        return True

    def save(self, mode):
        report = self.get_reports(self.args.date)
        fn = '{prefix}_{date}_{iter}.txt'.format(prefix="ING-SIMP", date=self.args.date, iter=1)
        if report['simp_report'] is not None:
            with open(fn, 'w') as f:
                f.write(report['simp_report'])
            logging.info(f"simp_report saved: {fn}")

        return True

    def send(self, mode):
        report = self.get_reports(self.args.date)
        fn = '{prefix}_{date}_{iter}.txt'.format(prefix="ING-SIMP", date=self.args.date, iter=1)
        if report['simp_report'] is not None:
            with open(fn, 'w') as f:
                f.write(report['simp_report'])

            logging.info(f"simp_report saved: {fn}")

            send_from = self.args.email
            send_to = self.args.email
            subject = "ING SIMP report at self.args.date"
            message = f'Dear Colleague, please find attached SIMP report generated for: {self.args.date}'
            files = [fn]
            send_mail(send_from, send_to, subject, message, files, server="localhost", port=25, username='', password='', use_tls=False, use_auth=False)
            
            logging.info(f"simp_report sent via email to: {send_to}")


        return True


def main():
    parser = argparse.ArgumentParser(description='ING SIMP report handler.')
    parser.add_argument("mode", choices=['get', 'save', 'send'], default='get', help=f'script mode (default: %(default)s)')
    parser.add_argument("-d", "--date", default=(datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"), help="report date (default: %(default)s)")
    parser.add_argument("-v", "--verbose", default=False, action='store_true', help="verbose mode (default: %(default)s)")
    parser.add_argument("-f", "--force", default=False, action='store_true', help="force mode - ignore errors (default: %(default)s)")
    parser.add_argument("-s", "--simpcode", default="105001617564", help="ING customer SIMP code (default: %(default)s)")
    parser.add_argument("-e", "--email", default="bankin@netforyou.pl", help="target email address that should recieve simp reports. (default: %(default)s)")
    args = parser.parse_args()

    sr = SimpReport(args)
    if args.mode in ['get', 'send', 'save']:
        result = False
        if args.mode == 'get':
            result = sr.get(args.mode)
        if args.mode == 'send':
            result = sr.send(args.mode)
        if args.mode == 'save':
            result = sr.save(args.mode)
        if result:
            sys.exit(0)
        sys.exit(12)  # exit with error in case problem in processing

    print(f'Mode: {args.mode} - is not yet implemented.')


if __name__ == '__main__':
    main()
