from lib.get_active import Pipeline
import os
from lib.pivot import PivotCsv
active=Pipeline()
active.get_patient_data(10)
path=os.path.dirname(os.path.realpath(__file__))
p=PivotCsv(path+'/input/active.csv' ,os.path.join("C:/Users/marce/OneDrive/Projects/Veredas","Pacientes Isabel_"))
p.process_folder(True)
#input("press any key to end")