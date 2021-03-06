#!/usr/bin/env python3

import os
import sys
import re
import logging
logging.basicConfig(level=logging.INFO)
import requests
import textwrap
import datetime
import argparse
import configparser
import base64
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
        self.reports = []

    def get_soap_request_body(self, msg_id, rpt_id, date, rpt_format='XML'):
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
                                       <urn2:RptFrmt>{rpt_format}</urn2:RptFrmt>
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
        return textwrap.dedent(soap_request_body_template).format(msg_id=msg_id, date=date, rpt_id=rpt_id, simp_code=self.simp_code, rpt_format=rpt_format)


    def send_soap_request(self, msg_id, rpt_id, date=None, rpt_format='XML'):
        status = True
        if date is None:
            date = self.simp_report_date
        soap_request = self.get_soap_request_body(msg_id, rpt_id, date, rpt_format)
        r = requests.post(url=wsdl_url, cert=cert_file, data=soap_request, headers=headers)
        soap_response = remove_namespace(ET.ElementTree(ET.fromstring(r.text)))
        if self.args.verbose:
            print(f"SOAP XML REQEST: {soap_request}")
            print(f"SOAP XML RESPONSE: {ET.tostring(soap_response)}")

        err = soap_response.xpath('//RuleDesc/text()')
        if len(err):
            if len(self.reports) == 0:  # following error should be printed in case of processing first item only.
                logging.error(f"Error processing soap request. date: {date} rpt_id: {rpt_id} ERR: {err}")
            logging.debug(f"SOAP request: {soap_request}")
            logging.debug(f"SOAP response: {ET.tostring(soap_response)}")
            status = False
            #if not self.args.force:
            #    logging.exception(f"There are no simp reports for: {self.simp_report_date}. {err}")
            #    raise SimpReportException(f"{err}")

        return soap_response, status


    def get_rpt_id(self, date=None, rpt_id=0):
        if date is None:
            date = self.simp_report_date
        msg_id = datetime.datetime.now().strftime('%s')
        xml_response, status = self.send_soap_request(msg_id, rpt_id, date, rpt_format='XML')
        if status is not False:
            rpt_id = xml_response.xpath('//GetMCReportResponse/Document/MCRpt/Rpt/RptId/EQ/text()')[0]

        return int(rpt_id), status


    def get_reports(self, date=None, raw=True):
        """
            Gets all SIMP reports for a given day.
        """

        self.reports = []  # reset reports table

        report = self.get_report(date, 0, raw)
        while report['status'] is True:
            next_report_id = report['rpt_id'] + 1
            report = self.get_report(date, next_report_id, raw)

        return report


    def get_report(self, date=None, rpt_id=0, raw=True):
        if date is None:
            date = self.simp_report_date

        data = {
            'simp_report': None,
            'date': date,
            'trans_count': 0,
            'rpt_id': 0,
            'status': True,
        }
        data['rpt_id'], data['status'] = self.get_rpt_id(data['date'], rpt_id)
        msg_id = datetime.datetime.now().strftime('%s')

        if raw is True:
            xml_data, status = self.send_soap_request(msg_id, data['rpt_id'], data['date'], rpt_format='RAW')
            data['simp_report'], data['simp_report_fn'] = self.process_report_file_xml(xml_data)
            trn = re.findall('"il.trn.:(\d+) ', data['simp_report'])
            if len(trn) > 0:
                data['trans_count'] = int(trn[0])
            self.reports.append(data)

        else:  # use XML format for SOAP responses
            xml_response, status = self.send_soap_request(msg_id, data['rpt_id'], data['date'])
            ntrys = xml_response.xpath('//Ntry')
            logging.debug("number of Ntry records:", len(ntrys))
            data['trans_count'] = len(ntrys)
            data['simp_report_fn'] = '{prefix}_{date}_{iter}.txt'.format(prefix="ING-SIMP", date=self.args.date, iter=data['rpt_id'])
            if len(ntrys) > 0:
                data['simp_report'] = self.ntrys2simp(ntrys)
            else:
                data['simp_report'] = ''
                #logging.info(f"There are no simp reports for date: {data['date']} rpt_id: {data['rpt_id']}")
            self.reports.append(data)

        return data


    def process_report_file_xml(self, xml_data):
        """
        <Envelope>
          <Body>
            <GetMCReportResponse>
              <Document>
                <Rpt>
                  <MsgId>
                    <Id>ING_CCS_MC_20211013_173050769</Id>
                  </MsgId>
                  <RptDtls>
                    <RptSts>R</RptSts>
                    <RptNm>netforyou_202109172</RptNm>
                    <RptCreDt>2021-09-17</RptCreDt>
                    <RptType>SIMP_FILE</RptType>
                    <RptSize>312</RptSize>
                    <RptFile>EPNFNJTCVAyPOjEwDNTAwEMTYxNDzU2NDCAOwNMDTAwMDAEwMDANwMDTAwLDIwMjEtMDktMTcNCjY4MTA1MDAxNjE3NTY0MDAwMDAwMDAwMDA0LDg3MDAsQyxQTE4sMjAyMS0wOS0xNyxVWk4sOTcyMDE4NzAwMjc4LCwiNzgxMTYwMjIwMjAwMDAwMDA0NjI2NDA3MDYiLCJMQUJFVFNLSSBTVEFOSVNMQVYiLCIiLCJXUtNCTEVXSUNFIFVMIFNURUZBTkEgQkFUT1JFR08gN003IiwiNTUtMzMwICAgIE1JyktJTklBIiwiUHJ6ZWxldyBrcmFqb3d5IC0gTkVDSU9SIiwiIiwiIiwiIixFLCwyMDIxLTA5LTE2DQo8L1NJTVAyPiJpbC50cm4uOjEgd2FydC50cm4uOjg3LjAwIg0K</RptFile>
                    <FileChecksum>B5FA5C2F7EFFFE9ABD7FBD930E847C06</FileChecksum>
                  </RptDtls>
                </Rpt>
              </Document>
            </GetMCReportResponse>
          </Body>
        </Envelope>
        """

        rpt = xml_data.xpath('//RptDtls')
        try:
            encoded_data = rpt[0].xpath('./RptFile')[0].text
            simp_report = base64.b64decode(encoded_data).decode(self.args.bank_encoding)
        except IndexError:
            simp_report = ''
        try:
            simp_report_fn = rpt[0].xpath('./RptNm')[0].text + '.txt'
        except IndexError:
            simp_report_fn = 'temp_file_{date}.txt'.format(date=self.args.date)

        return simp_report, simp_report_fn


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
            #data['tr_id'] = datetime.datetime.now().strftime('%s%f')
            data['tr_id'] = ntry.xpath('./Ref/TxRef/text()')[0][:-1]
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

            line = f"""{data['acccount_id']},{data['cammount']},{data['sgn']},{data['currency']},{data['booking_date']},UZN,{data['tr_id']},,"{data['payer_id']}","{data['payer_name1']}","{data['payer_name2']}","{data['payer_name3']}","{data['payer_name4']}","{data['memo']}","","","",{data['tr_src']},,{data['tr_date']}\n"""
            lines += line
 
        #print(data)
        template_header = f"""<SIMP2>666,{data['booking_date']}\n"""
        template_footer = f"""</SIMP2>"il.trn.:{data['tr_count']} wart.trn.:{data['tr_total']}"\n"""

        ret = template_header + lines + template_footer
        
        return ret


    def get(self, mode):
        self.get_reports(self.args.date)

        for report in self.reports:
            if report['status'] is True and report['trans_count'] > 0:
                print(report['simp_report'])

        return True


    def save(self, mode):
        self.get_reports(self.args.date)

        for report in self.reports:
            if report['status'] is True and report['trans_count'] > 0:
                fn = report['simp_report_fn']
                with open(fn, 'w') as f:
                    f.write(report['simp_report'])
                logging.info(f"simp_report saved: {fn}")

        return True


    def send(self, mode):
        self.get_reports(self.args.date)

        subject = f"ING SIMP report at {self.args.date}"
        message = f'Dear Colleague, please find attached SIMP report generated for: {self.args.date}'
        send_to = [self.args.mail_to, self.args.mail_from]
        files = []
        for report in self.reports:
            if report['status'] is True and report['trans_count'] > 0:
                fn = report['simp_report_fn']
                with open(fn, 'w') as f:
                    f.write(report['simp_report'])
                logging.info(f"simp_report saved: {fn}")
                files.append(fn)

        if len(files) > 0:
            send_mail(self.args.mail_from, send_to, subject, message, files, self.args.mail_host, self.args.mail_port, self.args.mail_user, self.args.mail_pass, use_tls=True, use_auth=True)
            logging.info(f"simp_report sent via email to: {send_to}")

        return True


def main():
    cparser = argparse.ArgumentParser(  # pre instance of argument parser - used to parse config file...
        description=__doc__,
        add_help=False,  # Turn off help, so we print all options in response to -h
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    cparser.add_argument("-c", "--conf_file", default=os.path.abspath(sys.argv[0]).replace('.py', '.conf'), help="Specify config file (default: %(default)s)")

    args, remaining_argv = cparser.parse_known_args()  # parse known arguments to include defaults from config file
    defaults = { "option":"default" }
    if args.conf_file:
        config = configparser.SafeConfigParser()
        config.read([args.conf_file])
        defaults.update(dict(config.items("defaults")))

    parser = argparse.ArgumentParser(
        parents=[cparser],
        description='ING SIMP report handler.',
        formatter_class=argparse.RawTextHelpFormatter,
    )  # Inherit options from config_parser
    parser.set_defaults(**defaults)

    parser.add_argument("mode", choices=['get', 'save', 'send'], default='get', help=f'script mode (default: %(default)s)')
    parser.add_argument("-v", "--verbose", default=False, action='store_true', help="verbose mode (default: %(default)s)")
    parser.add_argument("-f", "--force", default=False, action='store_true', help="force mode - ignore errors (default: %(default)s)")
    parser.add_argument("-d", "--date", default=(datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"), help="report date (default: %(default)s)")
    parser.add_argument("-s", "--simpcode", default=105001617564, help="ING customer SIMP code (default: %(default)s)")
    parser.add_argument("--mail_to", help="target email address that should recieve simp reports. (default: %(default)s)")
    parser.add_argument("--mail_from", help="source email address from which email is sent. (default: %(default)s)")
    parser.add_argument("--mail_host", help="mail server hostname. (default: %(default)s)")
    parser.add_argument("--mail_port", help="mail server port number. (default: %(default)s)")
    parser.add_argument("--mail_user", help="mail server user. (default: %(default)s)")
    parser.add_argument("--mail_pass", help="mail server password. (default: %(default)s)")
    parser.add_argument("--bank_encoding", default='ISO-8859-2', help="encoding used by the bank. (default: %(default)s)")

    args = parser.parse_args(remaining_argv)

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
