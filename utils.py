import numpy as np
import pandas as pd

columns_list = [
    'IDProtocollo',
    'Gruppo',
    'DataOraIncidente',
    'Localizzazione1',
    'STRADA1',
    'Localizzazione2',
    'STRADA2',
    'Strada02',
    'Chilometrica',
    'DaSpecificare',
    'NaturaIncidente',
    'ParticolaritaStrade',
    'TipoStrada',
    'FondoStradale',
    'Pavimentazione',
    'Segnaletica',
    'CondizioneAtmosferica',
    'Traffico',
    'Visibilità',
    'Illuminazione',
    'NUM_FERITI',
    'NUM_RISERVATA',
    'NUM_MORTI',
    'NUM_ILLESI',
    'LONGITUDINE',
    'LATITUDINE',
    'Confermato',
    'Progressivo',
    'TipoVeicolo',
    'StatoVeicolo',
    'Marca',
    'Modello',
    'TipoPersona',
    'AnnoNascita',
    'Sesso',
    'TipoLesione',
    'Deceduto',
    'DecedutoDopo',
    'CinturaCascoUtilizzato',
    'Airbag'
]

##################################################
# Create a single dataframe
"""
# Join the records into a single dataset
"""


def join_dataframes(path, *args):
    frames = []
    for arg in args:
        frame = pd.read_csv(arg, sep=';', encoding='latin-1', dtype='str', parse_dates=True, na_values='null',
                            names=columns_list, header=0)
        frames.append(frame)
    result = pd.concat(frames, ignore_index=True).reset_index()
    result = result.drop('index', 1)
    result.to_pickle(path)

##################################################
# Format columns and assign them to dataframe
"""
# Format columns according to our needs
"""


def assign_columns(dataframe):
    for column in dataframe.columns:
        if column == columns_list[0]:
            # Assign IDProtocollo
            dataframe[column] = dataframe[column].apply(lambda x: str(x).split(',')[0])
        elif column == columns_list[1]:
            # Assign Gruppo
            dataframe[column] = dataframe[column].apply(lambda x: str(x).split(',')[0])
        elif column == columns_list[2]:
            # Assign DataOraIncidente
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[3]:
            # Assign Localizzazione1
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[4]:
            # Assign STRADA1
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[5]:
            # Assign Localizzazione2
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[6]:
            # Assign STRADA2
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[7]:
            # Assign Strada02
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[8]:
            # Assign Chilometrica
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[9]:
            # Assign DaSpecificare
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[10]:
            # Assign NaturaIncidente
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[11]:
            # Assign ParticolaritàStrade
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[12]:
            # Assign TipoStrada
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[13]:
            # Assign FondoStradale
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[14]:
            # Assign Pavimentazione
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[15]:
            # Assign Segnaletica
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[16]:
            # Assign CondizioneAtmosferica
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[17]:
            # Assign Traffico
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[18]:
            # Assign Visibilità
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[19]:
            # Assign Illuminazione
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[20]:
            # Assign NUM_FERITI
            dataframe[column] = dataframe[column].apply(lambda x: str(x).split(',')[0])
            dataframe[column] = dataframe[column].apply(lambda x: x if x is not '' else '0')
        elif column == columns_list[21]:
            # Assign NUM_RISERVATA
            dataframe[column] = dataframe[column].apply(lambda x: str(x).split(',')[0])
            dataframe[column] = dataframe[column].apply(lambda x: x if x is not '' else '0')
        elif column == columns_list[22]:
            # Assign NUM_MORTI
            dataframe[column] = dataframe[column].apply(lambda x: str(x).split(',')[0])
            dataframe[column] = dataframe[column].apply(lambda x: x if x is not '' else '0')
        elif column == columns_list[23]:
            # Assign NUM_ILLESI
            dataframe[column] = dataframe[column].apply(lambda x: str(x).split(',')[0])
            dataframe[column] = dataframe[column].apply(lambda x: x if x is not '' else '0')
        elif column == columns_list[24]:
            # Assign LONGITUDINE
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[25]:
            # Assign LATITUDINE
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[26]:
            # Assign Confermato
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[27]:
            # Assign Progressivo
            dataframe[column] = dataframe[column].apply(lambda x: str(x).split(',')[0])
            dataframe[column] = dataframe[column].apply(lambda x: x if x is not '' else '1')
        elif column == columns_list[28]:
            # Assign TipoVeicolo
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[29]:
            # Assign StatoVeicolo
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[30]:
            # Assign Marca
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[31]:
            # Assign Modello
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[32]:
            # Assign TipoPersona
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[33]:
            # Assign AnnoNascita
            dataframe[column] = dataframe[column].apply(lambda x: str(x).split(',')[0])
            dataframe[column] = dataframe[column].apply(lambda x: x if x is not '' else 'nan')
        elif column == columns_list[34]:
            # Assign Sesso
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[35]:
            # Assign TipoLesione
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[36]:
            # Assign Deceduto
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[37]:
            # Assign DecedutoDopo
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[38]:
            # Assign CinturaCascoUtilizzato
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        elif column == columns_list[39]:
            # Assign Airbag
            dataframe[column] = dataframe[column].apply(lambda x: str(x))
        # Create NaN values again
        dataframe = dataframe.replace('nan', np.NaN)
    return dataframe

##################################################
# Remove columns we don't need
"""
# Gruppo is not needed
# Localizzazione 1 is not needed
# STRADA1 is not needed
# Localizzazione2 is not needed
# STRADA2 is not needed
# Strada02 is not needed
# Chilometrica is not needed
# DaSpecificare is not needed
# LONGITUDINE is not needed
# LATITUDINE is not needed
# Confermato is not needed
# StatoVeicolo is not needed
# CinturaCascoUtilizzato is not needed
# Airbag is not needed
"""


def remove_columns(dataframe):
    # Drop Gruppo
    dataframe = dataframe.drop(columns_list[1], 1)
    # Drop Localizzazione1
    dataframe = dataframe.drop(columns_list[3], 1)
    # Drop STRADA1
    dataframe = dataframe.drop(columns_list[4], 1)
    # Drop Localizzazione2
    dataframe = dataframe.drop(columns_list[5], 1)
    # Drop STRADA2
    dataframe = dataframe.drop(columns_list[6], 1)
    # Drop Strada02
    dataframe = dataframe.drop(columns_list[7], 1)
    # Drop Chilometrica
    dataframe = dataframe.drop(columns_list[8], 1)
    # Drop DaSpecificare
    dataframe = dataframe.drop(columns_list[9], 1)
    # Drop LONGITUDINE
    dataframe = dataframe.drop(columns_list[24], 1)
    # Drop LATITUDINE
    dataframe = dataframe.drop(columns_list[25], 1)
    # Drop Confermato
    dataframe = dataframe.drop(columns_list[26], 1)
    # Drop StatoVeicolo
    dataframe = dataframe.drop(columns_list[29], 1)
    # Drop Marca
    dataframe = dataframe.drop(columns_list[30], 1)
    # Drop Modello
    dataframe = dataframe.drop(columns_list[31], 1)
    # Drop CinturaCascoUtilizzato
    dataframe = dataframe.drop(columns_list[38], 1)
    # Drop Airbag
    dataframe = dataframe.drop(columns_list[39], 1)
    return dataframe

##################################################
# Fix broken values
"""
# AnnoNascita has broken values
"""


def fix_columns(dataframe):
    # Find broken AnnoNascita values
    indices_AnnoNascita = []
    for ind, elem in enumerate(dataframe['AnnoNascita']):
        # AnnoNascita is missing
        if elem is np.NaN:
            indices_AnnoNascita.append(ind)
        # AnnoNascita is zero
        elif int(elem) == 0:
            indices_AnnoNascita.append(ind)
        # AnnoNascita is bad inserted
        elif int(elem) > 2014:
            indices_AnnoNascita.append(ind)
    # Find corresponding IDProtocollo values
    values_IDProtocollo = dataframe.iloc[indices_AnnoNascita]['IDProtocollo'].tolist()
    # Remove all the entries which share broken values by IDProtocollo
    indices_IDProtoccolo = np.where(dataframe['IDProtocollo'].isin(values_IDProtocollo))[0]
    indices = [i for i in range(dataframe.shape[0]) if i not in indices_IDProtoccolo]
    dataframe = dataframe.iloc[indices, :].reset_index()
    dataframe = dataframe.drop('index', 1)
    return dataframe

##################################################
# Use DataOraIncidente column to produce new categories
"""
# Divide hours into six categories
# Divide days into three categories
# Use months as a new category
"""


def expand_DataOraIncidente(dataframe):
    periods = []
    months = []
    times = []
    for date in dataframe['DataOraIncidente']:
        # Parse date field
        calendar = date.split(' ')[0]
        day = calendar.split('/')[0]
        month = calendar.split('/')[1]
        time = date.split(' ')[1]
        hour = time.split(':')[0]
        # Create a new category for the period
        if int(day) <= 10:
            periods.append('InizioMese')
        elif int(day) <= 20:
            periods.append('MetaMese')
        elif int(day) <= 31:
            periods.append('FineMese')
        # Use months as they are
        months.append(month)
        # Create a new category for time
        if int(hour) <= 3:
            times.append('SeraTardi')
        elif int(hour) <= 7:
            times.append('MattinaPresto')
        elif int(hour) <= 11:
            times.append('MattinaTardi')
        elif int(hour) <= 15:
            times.append('PomeriggioPresto')
        elif int(hour) <= 19:
            times.append('PomeriggioTardi')
        elif int(hour) <= 23:
            times.append('SeraPresto')
    dataframe = dataframe.drop('DataOraIncidente', 1)
    dataframe['FaseGiorno'] = times
    #dataframe['FaseMese'] = periods
    #dataframe['Mese'] = months
    return dataframe

##################################################
# Use NUM columns to produce a new category
"""
# Combine NUM_FERITI NUM_RISERVATA NUM_MORTI NUM_ILLESI
# to obtain a new category
"""


def expand_NUM(dataframe):
    series = pd.to_numeric(dataframe['NUM_FERITI']) + pd.to_numeric(dataframe['NUM_RISERVATA']) + \
             pd.to_numeric(dataframe['NUM_MORTI']) + pd.to_numeric(dataframe['NUM_ILLESI'])
    dataframe = dataframe.drop('NUM_FERITI', 1)
    dataframe = dataframe.drop('NUM_RISERVATA', 1)
    dataframe = dataframe.drop('NUM_MORTI', 1)
    dataframe = dataframe.drop('NUM_ILLESI', 1)
    dimensions = []
    for dimension in series:
        if dimension <= 3:
            dimensions.append('Piccolo')
        elif dimension <= 6:
            dimensions.append('Medio')
        else:
            dimensions.append('Grande')
    dataframe['DimensioneIncidente'] = dimensions
    return dataframe

##################################################
# Use DecedutoDopo to gain more information on Deceduto
"""
# Use DecedutoDopo to correct the column TipoLesione
"""
DecedutoDopo_dict = {
    'DECEDUTO ENTRO 2 MESI': 'Deceduto dopo',
    'DECEDUTO ENTRO 15 GIORNI': 'Deceduto dopo',
    'DECEDUTO ENTRO LE DODICI ORE': 'Deceduto dopo',
    'DECEDUTO ENTRO 3 MESI': 'Deceduto dopo',
    'DECEDUTO ENTRO 6 MESI': 'Deceduto dopo',
    'DECEDUTO ENTRO IL QUINTO GIORNO': 'Deceduto dopo',
    'DECEDUTO ENTRO IL QUARTO GIORNO': 'Deceduto dopo',
    'DECEDUTO DA 25 A 48 ORE DOPO': 'Deceduto dopo',
    'DECEDUTO ENTRO IL TERZO GIORNO': 'Deceduto dopo',
    'DECEDUTO ENTRO 1 MESE': 'Deceduto dopo',
    'DECEDUTO DA 13 A 24 ORE DOPO': 'Deceduto dopo',
    'DECEDUTO ENTRO SETTIMO GIORNO': 'Deceduto dopo'
}


def expand_DecedutoDopo(dataframe):
    for ind, elem in enumerate(dataframe['DecedutoDopo']):
        if elem in DecedutoDopo_dict:
            dataframe['TipoLesione'][ind] = DecedutoDopo_dict[elem]
    dataframe = dataframe.drop('DecedutoDopo', 1)
    dataframe = dataframe.drop('Deceduto', 1)
    return dataframe

##################################################
# Adjust NaturaIncidente column
"""
# NaturaIncidente has 70718 on 70718 values
# NaturaIncidente needs to be mapped with fewer categories
"""
NaturaIncidente_dict = {
    'Scontro frontale/laterale SX fra veicoli in marcia': 'MarciaVsMarcia',
    'Scontro laterale fra veicoli in marcia': 'MarciaVsMarcia',
    'Scontro frontale/laterale DX fra veicoli in marcia': 'MarciaVsMarcia',
    'Scontro frontale fra veicoli in marcia': 'MarciaVsMarcia',
    'Veicolo in marcia contro ostacolo accidentale': 'MarciaVsOstacolo',
    'Veicolo in marcia contro ostacolo fisso': 'MarciaVsOstacolo',
    'Investimento di pedone': 'Investimento',
    'Tamponamento': 'MarciaVsFermo',
    'Veicolo in marcia contro veicolo in sosta': 'MarciaVsFermo',
    'Veicolo in marcia contro veicoli in sosta': 'MarciaVsFermo',
    'Veicolo in marcia contro veicoli fermi': 'MarciaVsFermo',
    'Tamponamento Multiplo': 'Multiplo',
    'Veicolo in marcia contro veicolo fermo': 'MarciaVsFermo',
    'Ribaltamento senza urto contro ostacolo fisso': 'Ribaltamento',
    'Infortunio per sola frenata improvvisa': 'Infortunio',
    'Fuoriuscita dalla sede stradale': 'Fuoriuscita',
    'Infortunio per caduta del veicolo': 'Infortunio',
    'Veicolo in marcia contro veicolo arresto': 'MarciaVsFermo',
    'Veicoli in marcia contro veicolo fermo': 'MarciaVsFermo',
    'Veicolo in marcia contro veicoli in arresto': 'MarciaVsFermo',
    'Veicoli in marcia contro veicoli fermi': 'MarciaVsFermo',
    'Veicolo in marcia contro treno': 'MarciaVsOstacolo'
}


def adjust_NaturaIncidente(dataframe):
    dataframe['NaturaIncidente'] = dataframe['NaturaIncidente'].map(NaturaIncidente_dict)
    return dataframe

##################################################
# Adjust ParticolaritaStrade column
"""
# ParticolaritaStrade has 70718 on 70718 values
# ParticolaritaStrade needs to be mapped with fewer categories
"""
ParticolaritaStrade_dict = {
    'Rettilineo': 'Rettilineo',
    'Incrocio': 'Incrocio',
    'Intersezione semaforizzata': 'IntersezioneControllata',
    'Intersezione stradale segnalata': 'IntersezioneControllata',
    'Curva senza visuale libera': 'Curva',
    'Curva a visuale libera': 'Curva',
    'Rotatoria': 'Curva',
    'Intersezione non regolata/non segnalata': 'IntersezioneNonControllata',
    'Pendenza': 'Altro',
    'Dosso': 'Altro',
    'Sottopasso illuminato': 'Galleria',
    'Galleria illuminata': 'Galleria',
    'Cavalcavia': 'Rettilineo',
    'Sottopasso non illuminato': 'Galleria',
    'Strettoia': 'Altro',
    'Intersezione regolata dal vigile': 'IntersezioneControllata',
    'Galleria non illuminata': 'Galleria',
    'Cunetta': 'Altro',
    'Pianeggiante': 'Rettilineo',
    'Passaggio a livello (custodito)': 'Altro'
}


def adjust_ParticolaritaStrade(dataframe):
    dataframe['ParticolaritaStrade'] = dataframe['ParticolaritaStrade'].map(ParticolaritaStrade_dict)
    return dataframe

##################################################
# Adjust FondoStradale column
"""
# FondoStradale has 70718 on 70718 values
# FondoStradale needs to be mapped with fewer categories
"""
FondoStradale_dict = {
    'Asciutto': 'Asciutto',
    'Bagnato (pioggia)': 'Bagnato',
    'Bagnato (umidita in atto)': 'Bagnato',
    'Bagnato (brina)': 'Bagnato',
    'Viscido da liquidi oleosi': 'Altro',
    'Sdrucciolevole (fango)': 'Altro',
    'Sdrucciolevole (terriccio)': 'Altro',
    'Sdrucciolevole (pietrisco)': 'Altro',
    'Sdrucciolevole (sabbia)': 'Altro',
    'Ghiacciato': 'Altro'
}


def adjust_FondoStradale(dataframe):
    dataframe['FondoStradale'] = dataframe['FondoStradale'].map(FondoStradale_dict)
    return dataframe

##################################################
# Adjust Pavimentazione column
"""
# Pavimentazione has 70718 on 70718 values
# Pavimentazione needs to be mapped with fewer categories
"""
Pavimentazione_dict = {
    'Asfaltata': 'Regolare',
    'Con buche': 'Dissestata',
    'Strada pavimentata dissestata': 'Dissestata',
    'Acciotolata': 'Lastricata',
    'Lastricata': 'Lastricata',
    'In cubetti di porfido': 'Lastricata',
    'Sterrata': 'Dissestata',
    'In conglomerato cementizio': 'Regolare',
    'Inghiaiata': 'Dissestata',
    'Fondo naturale': 'Dissestata'
}


def adjust_Pavimentazione(dataframe):
    dataframe['Pavimentazione'] = dataframe['Pavimentazione'].map(Pavimentazione_dict)
    return dataframe

##################################################
# Adjust CondizioneAtmosferica column
"""
# Pavimentazione has 70718 on 70718 values
# Pavimentazione needs to be mapped with fewer categories
"""
CondizioneAtmosferica_dict = {
    'Sereno': 'Sereno',
    'Nebbia': 'Nuvoloso',
    'Nuvoloso': 'Nuvoloso',
    'Pioggia in atto': 'Precipitazioni',
    'Grandine in atto': 'Precipitazioni',
    'Foschia': 'Nuvoloso',
    'Vento forte': 'Nuvoloso',
    'Sole radente': 'Sereno'
}


def adjust_CondizioneAtmosferica(dataframe):
    dataframe['CondizioneAtmosferica'] = dataframe['CondizioneAtmosferica'].map(CondizioneAtmosferica_dict)
    return dataframe

##################################################
# Adjust Traffico column
"""
# Traffico has 70696 on 70718 values
# Traffico has null which needs to be replaced
"""


def adjust_Traffico(dataframe):
    dataframe['Traffico'] = dataframe['Traffico'].fillna(value='Normale')
    return dataframe

##################################################
# Adjust TipoVeicolo column
"""
# TipoVeicolo has 68533 on 70718 values
# TipoVeicolo needs to be mapped with fewer categories
"""
TipoVeicolo_dict = {
    'Autovettura privata': 'Autovettura',
    'Motociclo a solo': 'Motociclo',
    'Autocarro inferiore 35 q.': 'Autocarro',
    'Autovettura pubblica': 'Autovettura',
    'Autocarro superiore 35 q.': 'Autocarro',
    'Motociclo con passeggero': 'Motociclo',
    'Autovettura di polizia': 'Autovettura',
    'Ciclomotore': 'Ciclomotore',
    'Quadriciclo leggero': 'Autovettura',
    'Autobus urbano': 'Autobus',
    'Altro': 'Altro',
    'Autobus di linea': 'Autobus',
    'Velocipede': 'Bicicletta',
    'Autocaravan': 'Autocaravan',
    'Veicolo a trazione animale': 'Altro',
    'Autoambulanza': 'Soccorso',
    'Autoarticolato': 'Autotreno',
    'Veicolo speciale': 'Speciale',
    'Vettura tranviaria': 'Tram',
    'Autobus turistico': 'Autobus',
    'Autovettura di soccorso': 'Soccorso',
    'Motociclo di polizia': 'Motociclo',
    'Autopompa': 'Soccorso',
    'Autoveicolo trasp.promisc.': 'Autocarro',
    'Autoambulanza in servizio': 'Soccorso',
    'Macchina operatrice': 'Opera',
    'Ciclomotore con passeggero': 'Ciclomotore',
    'Trattore stradale': 'Opera',
    'Autogru': 'Opera',
    'Motrice': 'Autotreno',
    'Autotreno': 'Autotreno',
    'Autobus in extraurbana': 'Autobus',
    'Motocarro': 'Ciclomotore',
    'Motocarrozzetta': 'Ciclomotore',
    'Autovettura con rimorchio': 'Autovettura',
    'Filobus': 'Tram',
    'Rimorchio': 'Autotreno',
    'Treno': 'Altro',
    'Autosnodato': 'Autobus',
    'Trattore agricolo': 'Opera',
    'Veicolo a braccia': 'Altro',
    'A piedi': 'Piedi'
}


def adjust_TipoVeicolo(dataframe):
    dataframe['TipoVeicolo'] = dataframe['TipoVeicolo'].fillna(value='A piedi')
    dataframe['TipoVeicolo'] = dataframe['TipoVeicolo'].map(TipoVeicolo_dict)
    return dataframe

##################################################
# Adjust TipoPersona column
"""
# TipoPersona has 70718 on 70718 values
# TipoPersona needs to be mapped with fewer categories
"""
TipoPersona_dict = {
    'Conducente': 'Conducente',
    'Passeggero': 'Passeggero',
    'Pedone': 'Pedone',
    'Pedone sconosciuto': 'Pedone',
    'Passeggero Istruttore': 'Passeggero',
    'Passeggero non identificato': 'Passeggero'
}


def adjust_TipoPersona(dataframe):
    dataframe['TipoPersona'] = dataframe['TipoPersona'].map(TipoPersona_dict)
    return dataframe

##################################################
# Adjust AnnoNascita column
"""
# AnnoNascita has 70717 on 70718 values
# AnnoNascita needs to be mapped with fewer categories
"""


def adjust_AnnoNascita(dataframe):
    ages = []
    for year in dataframe['AnnoNascita']:
        age = 2014 - int(year)
        if age <= 5:
            ages.append('Neonato')
        elif age <= 15:
            ages.append('Bambino')
        elif age <= 25:
            ages.append('Ragazzo')
        elif age <= 45:
            ages.append('Adulto')
        elif age <= 65:
            ages.append('Anziano')
        else:
            ages.append('NonnoSimpson')
    dataframe = dataframe.drop('AnnoNascita', 1)
    dataframe['FasciaEta'] = ages
    return dataframe

##################################################
# Adjust Sesso columns
"""
# Sesso has 70645 on 70718 values
# Sesso has nulls which needs to be replaced
"""


def adjust_Sesso(dataframe):
    m_freq = dataframe['Sesso'].value_counts()['M']
    f_freq = dataframe['Sesso'].value_counts()['F']
    m_prob = m_freq / (m_freq + f_freq)
    f_prob = f_freq / (m_freq + f_freq)
    threshold = 0
    head = np.NaN
    tail = np.NaN
    if m_prob > f_prob:
        threshold = f_prob
        head = 'M'
        tail = 'F'
    else:
        threshold = m_prob
        head = 'F'
        tail = 'M'
    indices_Sesso = np.where(dataframe['Sesso'].isnull())[0]
    for ind in indices_Sesso:
        if (np.random.random() > threshold):
            dataframe['Sesso'][ind] = head
        else:
            dataframe['Sesso'][ind] = tail
    return dataframe

##################################################
# Adjust TipoLesione column
"""
# TipoLesione has 70718 on 70718 values
# TipoLesione needs to be mapped with fewer categories
"""
TipoLesione_dict = {
    'Illeso': 'Illeso',
    'Rimandato': 'Rimandato',
    'Rifiuta cure immediate': 'Rimandato',
    'Ricoverato': 'Ricoverato',
    'Prognosi riservata': 'Ricoverato',
    'Deceduto sul posto': 'Deceduto',
    'Deceduto durante prime cure': 'Deceduto',
    'Deceduto durante trasporto': 'Deceduto',
    'Deceduto dopo': 'Deceduto'
}


def adjust_TipoLesione(dataframe):
    dataframe['TipoLesione'] = dataframe['TipoLesione'].map(TipoLesione_dict)
    return dataframe

##################################################
# TODO
# Improve NaturaIncidente column
# Improve ParticolaritaStrade column
# Improve TipoVeicolo column
# Merge CinturaCascoUtilizzato and Airbag columns
