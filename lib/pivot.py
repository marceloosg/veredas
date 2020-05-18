from pandas import DataFrame
import pandas as pd
import datetime
import os
import numpy as np

class PivotCsv:
    def __init__(self, input_path, output_dir_path, cutoff=120):
        self.df = pd.read_csv(input_path, sep=";")
        self.patient_ids = self.df.idpaciente.unique()
        self.cutoff = datetime.datetime.today().date() - datetime.timedelta(days=cutoff)
        self.output_dir_path = output_dir_path

    @staticmethod
    def to_date(s):
        return datetime.datetime.strptime(s, '%Y-%m-%d').date()

    def process_worksheet(self, patient_id, symptomType):
        chunk = self.df.query('idpaciente == @patient_id & tipo == @symptomType')
        chunk = pd.DataFrame(chunk.loc[:, ["idsintoma", 'valor', 'data', 'sintoma']])
        chunk['data'] = [self.to_date(s) for s in chunk.data.values]
        chunk.set_index(['idsintoma'], inplace=True)
        chunk = pd.DataFrame(chunk.query("data >= @self.cutoff"))
        symptoms = pd.DataFrame(chunk.loc[:, ["sintoma"]])
        symptoms.drop_duplicates('sintoma', inplace=True)
        out = chunk.pivot(columns='data', values='valor')
        final: DataFrame = symptoms.merge(out, left_index=True, right_index=True)
        return final

    def process_workbook(self, patient_id,progress=False):
        if progress:
            cnt = len(self.patient_ids)
            print("{}\n".format(np.where(patient_id==self.patient_ids)[0]/cnt))
        types = self.df.query("idpaciente == @patient_id").tipo.unique()
        worksheets = [(symptomType, self.process_worksheet(patient_id, symptomType)) for symptomType in types]
        patient_name = self.df.query('idpaciente == @patient_id').Nome.unique()[0]
        filename = str(patient_id) + "-" + patient_name.lower() + ".xlsx"
        out_path = os.path.join(self.output_dir_path, filename)
        writer = pd.ExcelWriter(out_path,
                                engine='xlsxwriter',
                                datetime_format='mmm d yyyy hh:mm:ss',
                                date_format='mmmm dd yyyy')
        for symptomType, worksheet in worksheets:
            worksheet.to_excel(writer, sheet_name=symptomType, startrow=0, header=True, index=True)
        writer.save()

    def process_folder(self,progress):
        [self.process_workbook(patient_id,progress) for patient_id in self.patient_ids]
