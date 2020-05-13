from lib.queryfactory import QueryFactory
import os


class Pipeline:

    def __init__(self, path="scripts"):
        self.path = path
        self.credentials = self.get_credentials()
        self.patient_query, self.sintomas_query, self.sintomas_diario_base_query, self.patient_base_query = self.get_queries()

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
        patient_base_query = 'select idpaciente, Nome from jpaciente where idpaciente in {}'

        return patient_query, sintomas_query, sintomas_diario_base_query, patient_base_query

    def get_patient_data(self):
        credentials = self.get_credentials()
        engine = QueryFactory(credentials)

        patient_list = engine.query(self.patient_query)
        sintomas = engine.query(self.sintomas_query)
        patient_ids = '{}'.format(list(patient_list.idpaciente.values)).replace('[', '(').replace(']', ')')
        sintomas_diario_query = self.sintomas_diario_base_query.format(patient_ids)
        sintomas_diario = engine.query(sintomas_diario_query)
        patient_data = engine.query(self.patient_base_query.format(patient_ids))
        active_data = sintomas_diario.merge(patient_data,
                                            how='inner',
                                            on='idpaciente').merge(sintomas,
                                                                   how='inner',
                                                                   on='idsintoma')
        fn = os.path.join('input', 'active.csv')
        active_data.to_csv(fn, sep=';', index=False)
