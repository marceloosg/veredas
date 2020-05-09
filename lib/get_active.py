from lib.queryfactory import QueryFactory
import os


def read_vars(file):
    file='mysql_'+file
    fn=os.path.join('scripts',file)
    out=""
    with open(fn,'r') as f:
        out=f.read().strip('\n')
    return out


def get_credentials(path='scripts'):
    vars = [f.split("_")[1] for f in os.listdir(path) if 'mysql' in f]
    credentials=dict([(f,read_vars(f)) for f in vars])
    return credentials


def get_queries(path='scripts'):
    pacient_query=""
    with open(os.path.join('scripts', 'get_patients.sql'),'r') as f:
        pacient_query=f.read()

    sintomas_query='select idsintomasGeral idsintoma, replace(Nome,";"," ") sintoma, tipo from jsintomasgeral'
    sintomas_diario_base_query="select idsintoma,valor,data,idpaciente " \
                               "from jsintomadiario " \
                               "where data is not null " \
                               "and data != '0000-00-00' " \
                               "and idpaciente in {}"
    pacient_base_query='select idpaciente, Nome from jpaciente where idpaciente in {}'

    return pacient_query, sintomas_query, sintomas_diario_base_query, pacient_base_query


def get_pacient_data(path='scripts'):
    credentials = get_credentials()
    engine = QueryFactory(credentials)

    pacient_query, query, sintomas_query, sintomas_diario_base_query, pacient_base_query= get_queries()
    pacient_list =engine.query(pacient_query)
    sintomas =engine.query(sintomas_query)
    pacient_ids='{}'.format(list(pacient_list.idpaciente.values)).replace('[','(').replace(']',')')
    sintomas_diario_query = sintomas_diario_base_query.format(pacient_ids)
    sintomas_diario =engine.query(sintomas_diario_query)
    pacient_data=engine.query(pacient_base_query.format(pacient_ids))
    active_data = sintomas_diario.merge(pacient_data,
                                        how='inner',
                                        on='idpaciente').merge(sintomas,
                                                               how='inner',
                                                               on='idsintoma')
    fn = os.path.join('input', 'active.csv')
    active_data.to_csv(fn, sep=';', index=False)

