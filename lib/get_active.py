from lib.queryfactory import QueryFactory
import os
import pandas as pd

class Pipeline:

    def __init__(self, path="scripts"):
        self.path = path
        self.credentials = self.get_credentials()
        self.patient_query, self.sintomas_query, self.sintomas_diario_base_query, self.patient_base_query = self.get_queries()
        self.engine = QueryFactory(self.credentials)


    @staticmethod
    def read_vars(file):
        file = 'mysql_' + file
        fn = os.path.join('scripts', file)
        out = ""
        with open(fn, 'r') as f:
            out = f.read().strip('\n')
        return out

    def get_credentials(self):
        vars = [f.split("_")[1] for f in os.listdir(self.path) if 'mysql' in f]
        return dict([(f, self.read_vars(f)) for f in vars])

    def get_queries(self):
        patient_query = ""
        with open(os.path.join(self.path, 'get_patients.sql'), 'r') as f:
            patient_query = f.read()

        sintomas_query = 'select idsintomasGeral idsintoma, replace(Nome,";"," ") sintoma, tipo from jsintomasgeral'
        sintomas_diario_base_query = "select idsintoma,valor,data,idpaciente " \
                                     "from jsintomadiario " \
                                     "where data is not null " \
                                     "and data != '0000-00-00' " \
                                     "and idpaciente in {}"
        patient_base_query = 'select distinct idpaciente, Nome from jpaciente where idpaciente in {}'

        return patient_query, sintomas_query, sintomas_diario_base_query, patient_base_query

    def get_page_patient_data(self,ids, sintomas):
        print("Retrieving ids {}".format(ids))
        patient_ids = '{}'.format(ids).replace('[', '(').replace(']', ')')
        sintomas_diario_query = self.sintomas_diario_base_query.format(patient_ids)
        print("\t sintomas diarios")
        sintomas_diario = self.engine.query(sintomas_diario_query)
        print("\t patient data")
        patient_data = self.engine.query(self.patient_base_query.format(patient_ids))
        active_data = sintomas_diario.merge(patient_data,
                                            how='inner',
                                            on='idpaciente').merge(sintomas,
                                                                   how='inner',
                                                                   on='idsintoma')
        return active_data

    @staticmethod
    def split(a, n):
        k, m = divmod(len(a), n)
        return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

    def get_patient_data(self,split=5):
        print("Retrieving Patient List")
        patient_list = self.engine.query(self.patient_query)
        print("Retrieving Sympthons List")
        sintomas = self.engine.query(self.sintomas_query)
        full_ids=list(patient_list.idpaciente.values)
        id_list = self.split(full_ids,split)
        glist = [id for id in id_list]
        print("Retrieving Paged data from List {}".format(glist))
        result_list=[self.get_page_patient_data(id_chunck, sintomas) for id_chunck in glist]
        #print(result_list)
        active_datas = pd.concat(result_list)
        print("Done")

        fn = os.path.join('input', 'active.csv')
        active_datas.to_csv(fn, sep=';', index=False)
