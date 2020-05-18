from lib.get_active import Pipeline
from time import time
active=Pipeline()
z=time()
print("Get Data {}".format(time()-z))
active.get_patient_data()
from lib.pivot import PivotCsv
print("Pivot {}".format(time()-z))
p=PivotCsv('input/active.csv' ,"Pacientes Isabel_")
p.process_folder(True)
print("End Pivot {}".format(time()-z))

