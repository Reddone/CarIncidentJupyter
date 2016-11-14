import os
import pandas as pd
import utils

# Prepare the dataset for the analysis
sem_path = r"0_Sem_2014"
sem1_path = r"1_Sem_2014.csv"
sem2_path = r"2_Sem_2014.csv"

if not os.path.isfile(sem_path):
    utils.join_dataframes(sem_path, sem1_path, sem2_path)
dataset = pd.read_pickle(sem_path)

# Assign correct values to columns
dataset = utils.assign_columns(dataset)
# Remove columns we don't need
dataset = utils.remove_columns(dataset)
# Fix broken values
dataset = utils.fix_columns(dataset)

# Create new features using DataOraIncidente column
dataset = utils.expand_DataOraIncidente(dataset)
# Create new features using NUM columns
dataset = utils.expand_NUM(dataset)
# Create new features using DecedutoDopo column
dataset = utils.expand_DecedutoDopo(dataset)

# Adjust NaturaIncidente column
dataset = utils.adjust_NaturaIncidente(dataset)
# Adjust ParticolaritaStrade column
dataset = utils.adjust_ParticolaritaStrade(dataset)
# Adjust FondoStradale column
dataset = utils.adjust_FondoStradale(dataset)
# Adjust Pavimentazione column
dataset = utils.adjust_Pavimentazione(dataset)
# Adjust CondizioneAtmosferica column
dataset = utils.adjust_CondizioneAtmosferica(dataset)
# Adjust Traffico column
dataset = utils.adjust_Traffico(dataset)
# Adjust TipoVeicolo column
dataset = utils.adjust_TipoVeicolo(dataset)
# Adjust TipoPersona column
dataset = utils.adjust_TipoPersona(dataset)
# Adjust AnnoNascita column
dataset = utils.adjust_AnnoNascita(dataset)
# Adjust Sesso column
dataset = utils.adjust_Sesso(dataset)
# Adjust TipoLesione column
dataset = utils.adjust_TipoLesione(dataset)

save_path = r"0_CarIncident_2014"
save_path_csv = r"0_CarIncident_2014.csv"
dataset.to_pickle(save_path)
dataset.to_csv(save_path_csv)
