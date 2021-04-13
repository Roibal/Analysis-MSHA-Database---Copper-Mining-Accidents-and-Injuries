"""
Created February 12, 2021 by Joaquin Roibal for ME 489
Team Members: Braci Chester, Julienne Mamaita Essomba, Carlha Barreto
Updated: 4/13/2021

The Purpose of this Document is to load Mine Accident Data from MSHA Website (DBF file)
into a format which can be used to analyze the data with graphs.

After loading from a DBF file, the data is analyzed using Pandas and data will be visualized
to inform Mine Safety Health Practices in the future and reduce accidents/injuries
"""

import pandas as pd
import numpy as np
from simpledbf import Dbf5
import matplotlib as mpl
import matplotlib.pyplot as plt
from collections import Counter

def main():
    print("Loading Mine Accident Data, 2010-2019")
    year_list = ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019'] #Years
    year_prod_list =['1110', '1110', '1170', '1250', '1360', '1380', '1430', '1260', '1220', '1300']  #Million Tonnnes of Copper produced per year
    inj_rate=[]
    num_total_accidents =[]
    num_total_fatal = []
    num_copper_accidents = []
    #MSHA Files must be downloaded and included in the same folder as python program
    mine_data = ['ai2010.dbf', 'ai2011.dbf', 'ai2012.dbf', 'ai2013.dbf', 'ai2014.dbf', 'ai2015.dbf', 'ai2016.dbf', 'ai2017.dbf', 'ai2018.dbf', 'ai2019.dbf']
    output = []
    days_lost_stats = []
    tot_days_lost = []
    tot_days_lost_exp = []
    state_rep = {1: 'Alabama', 2: 'Alaska', 4: 'Arizona', 5: 'Arkansas', 6: 'Colorado', 8: 'Colorado', 9: 'Connecticut', 10: 'Delaware',
                 11: 'D.C.', 12: 'Florida', 13: 'Georgia', 15: 'Hawaii', 16: 'Idaho', 17: 'Illinois', 18: 'Indiana', 19: 'Iowa', 20: 'Kansas', 21: 'Kentucky', 22: 'Louisiana', 23: 'Maine',
                 24: 'Maryland', 25: 'Massachusetts', 26: 'Michigan', 27: 'Minnesota', 28: 'Mississippi', 29: 'Missouri', 30: 'Montana',
                 31: 'Nebraska', 32: 'Nevada', 33: 'New Hampshire', 34: 'New Jersey', 35: 'New Mexico', 36: 'New York', 37: 'North Carolina',
                 38: 'North Dakota', 39: 'Ohio', 40: 'Oklahoma', 41: 'Oregon', 42: 'Pennsylvania', 44: 'Rhode Island', 45: 'South Carolina',
                 46: 'South Dakota', 47: 'Tennessee', 48: 'Texas', 49: 'Utah', 50: 'Vermont', 51: 'Virginia', 53: 'Washington',
                 54: 'West Virginia', 55: 'Wisconsin', 56: 'Wyoming', 60: 'American Samoa', 61: 'Panama Canal Zone', 66: 'Guam',
                 69: 'Northern Mariana Islands', 72: 'Puerto Rico', 78: 'Virgin Islands', 98: 'Contractor-No State'}
    atype_rep = {1: 'Struck',2:'Struck',3:'Struck',4:'Struck',5:'Struck',6:'Struck',7:'Struck',8: 'Struck', 9:'Fall',10:'Fall',
                 11:'Fall',12:'Fall',13:'Fall',14:'Fall',15:'Fall',16:'Fall',17:'Fall',18:'Fall',19: 'Fall', 20:'Caught',
                 21:'Caught',22:'Caught',23:'Caught',24: 'Caught', 25:'NEC',26: "NEC", 27:"Over-Exertion",28:"Over-Exertion",29:"Over-Exertion",30:"Over-Exertion", 31:"Contact",
                 32:"Contact",33:"Contact",34:"Contact",35: "Contact", 36:"Inhalation/Ingestion",37:"Inhalation/Ingestion",38: "Inhalation/Ingestion",
                 39:"Flash Burns",40: "Flash Burns", 41: "Drowning", 42:"Unclassified",43: "Unclassified", 44: "No Injuries"}

    state_count = []
    injury_count = []
    database_list = []
    df_acc_list = []
    for i, database in enumerate(mine_data):
        #Load all Data from MSHA DBF file into a Pandas Database
        #transform dbf into pandas dataframe: https://stackoverflow.com/questions/41898561/pandas-transform-a-dbf-table-into-a-dataframe
        dbf = Dbf5(database, codec='cp850')

        #csv_name = str(database[0:6]) + ".csv"     #Following lines of code are for converting from dbf to csv
        #dbf.to_csv(csv_name)
        #mine_data2.append(csv_name)

        num_total_accidents.append(dbf.numrec)       #Number of Records
        #print(dbf.fields)          #Number of Fields and info regarding fields (columns)
        #print(dbf.mem())           #Memory Requirements (RAM)
        df=dbf.to_dataframe()
        #df = pd.read_csv(database, usecols=range(0,60), encoding='cp850')  #add parse_dates = True

        #print(df.head(10))
        #print(df.columns)

        df_copper = df[df.SIC == int('10210')]   #Create Dataframe with Copper Ore Only
        tot_days_lost.append(df_copper["DAYSLOST"].sum())
        database_list.append(df_copper)
        df_fatal = df_copper[df_copper['DEGINJ']==int(1)]       #Fatal Injuries
        print("Number of Fatalities: ", len(df_fatal))
        print(df_fatal)
        num_total_fatal.append(len(df_fatal))
        df_copper_exp0_5 = df_copper[df_copper.EXPTOTAL<5]  #Select a subset of data into new data frame
        df_copper_exp5_10 = df_copper[df_copper.EXPTOTAL>5 & (df_copper.EXPTOTAL<10)]
        df_copper_exp10_ = df_copper[df_copper.EXPTOTAL>10]
        tot_days_05 = df_copper_exp0_5["DAYSLOST"].sum()
        tot_days_5_10 = df_copper_exp5_10["DAYSLOST"].sum()
        tot_days_10_ = df_copper_exp10_["DAYSLOST"].sum()
        tot_days_lost_exp.append([year_list[i], tot_days_05, tot_days_5_10, tot_days_10_])

        #df_copper = df_copper.DAYSTOTL
        num_copper_accidents.append(len(df_copper))

        #df_copper.groupby(['DISTRICT']).groups
        df_copper['STATE'].replace(state_rep, inplace=True)     #replacing variables with MSHA database code numbers
        df_copper['ATYPE'].replace(atype_rep, inplace=True)
        df_state_counts = pd.Series(df_copper['STATE']).value_counts()      #Counting Injury Classification
        df_accident_counts = pd.Series(df_copper['ATYPE']).value_counts()
        state_count.append([year_list[i], df_state_counts])                 #Entering into list data format
        injury_count.append([year_list[i], df_accident_counts])

        #fig, ax = plt.subplots()    #Comment in / out for single / multiple plots
        df_acc_list.append(df_accident_counts)
        print(df_accident_counts)

        plt.scatter(df_copper["EXPTOTAL"], df_copper["DAYSLOST"], label=str(year_list[i]))
        days_lost_stats.append([year_list[i], df_copper['AGE'].describe()])


    plt.title("Days Lost and Total Mining Experience. Copper Mining in US, 2010-2019")
    plt.xlabel("Years Worked at a Mine")
    plt.ylabel("Days Lost")
    plt.legend()
    plt.show()

    for i, x in enumerate(df_acc_list):
        plt.plot(x, label=str(year_list[i]), marker='o', linestyle='None')
    plt.title("Accident Type and Total Number, Copper Mining in US, 2010-2019")
    plt.xlabel("Accident Type")
    plt.ylabel("Number of Yearly Accidents")
    plt.legend()
    plt.show()

    #ax.set_xticklabels(df_state_counts.keys())


    for l, cop in enumerate(database_list):
        #Create a graph comparing age and Experience Total
        plt.scatter(cop['AGE'], cop["EXPTOTAL"], label=str(year_list[l]))   #Comparing Work Experience and Age
    plt.title("Comparing Age and Work Experience, US Copper Mining Industry 2010-2019")
    plt.xlabel("Age of Miner")
    plt.ylabel("Number of Years Experience")
    plt.legend()
    plt.show()

    for l, cop in enumerate(database_list):
        plt.scatter(cop['AGE'], cop["DAYSLOST"], label=str(year_list[l]))   #Comparing Age and Days Lost
    plt.title("Comparing Age and Days Lost, US Copper Mining Industry, 2010-2019")
    plt.xlabel("Age of Miner")
    plt.ylabel("Days Lost")
    plt.legend()
    plt.show()

    for l, cop in enumerate(database_list):
        plt.scatter(cop['ATYPE'], cop["DAYSLOST"], label=str(year_list[l]))   #Comparing Age and Days Lost
    plt.title("Comparing Accident Type and Days Lost, US Copper Mining Industry, 2010-2019")
    plt.xlabel("Accident Type")
    plt.ylabel("Days Lost")
    plt.legend()
    plt.show()


    for l, cop in enumerate(database_list):
       cop['AGE'].hist(label=str(year_list[i]), histtype ='step', linewidth=2) #Creating a Histogram of Miner Age
    plt.title("Histogram of Injured Miner Ages in US Copper Mining Industry, 2010-2019")
    plt.xlabel("Age of Injured Miner")
    plt.ylabel("Number of Miners Injured")
    plt.legend()
    plt.grid(False)
    plt.show()

    for l, cop in enumerate(database_list):
       cop['EXPTOTAL'].hist(label=str(year_list[i]), histtype ='step', linewidth=2) #Creating a Histogram of Miner Experience
    plt.title("Histogram of Injured Miner Experience in US Copper Mining Industry, 2010-2019")
    plt.xlabel("Experience of Injured Miner")
    plt.ylabel("Number of Miners Injured")
    plt.legend()
    plt.grid(False)
    plt.show()

    for l, cop in enumerate(database_list):
       cop['DAYSLOST'].hist(label=str(year_list[i]), histtype ='step', linewidth=2) #Creating a Histogram of Days Lost
    plt.title("Histogram of Days Lost per Injury Experience in US Copper Mining Industry, 2010-2019")
    plt.xlabel("Days Lost of Injured Miner")
    plt.ylabel("Number of Injuries")
    plt.legend()
    plt.grid(False)
    plt.show()

    fig, ax1 = plt.subplots()       #Create a double y axis plot which will illustrate fatalaties and total work days lost

    color = 'tab:red'
    ax1.set_xlabel('Years')
    ax1.set_ylabel('Total Fatalities', color=color)
    ax1.plot(year_list, num_total_fatal, color=color)
    ax1.tick_params(axis='y', labelcolor=color)

    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

    color = 'tab:blue'
    ax2.set_ylabel('Total Days Lost', color=color)  # we already handled the x-label with ax1
    ax2.plot(year_list, tot_days_lost, color=color)
    ax2.tick_params(axis='y', labelcolor=color)

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.title("Number of Fatalities and Total Days Missed in Copper Mining Accidents in US, 2010-2019")
    plt.show()

    #print(df.head())
    print("Year: ", year_list)
    print("Total Accidents: ", num_total_accidents)
    print("Total Copper Accidents: ", num_copper_accidents)
    #print(state_count)
    for i in range(0, len(year_list)):
        #A for list to calculate injury rate (num accidents / yearly production) for each year
        inj_rate.append(float(num_copper_accidents[i])/float(year_prod_list[i]))
    print(injury_count)
    print("Injury Rate: ", inj_rate)

    #To convert to CSV:
    #dbf.to_csv('test.csv')


    #Draw Graphs of Data (Total Accidents per Year & Total Copper Mine Accidents per Year)
    fig, ax1 = plt.subplots()

    color = 'tab:red'
    ax1.set_xlabel('Years')
    ax1.set_ylabel('Total Accidents', color=color)
    ax1.plot(year_list, num_total_accidents, color=color)
    ax1.tick_params(axis='y', labelcolor=color)

    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

    color = 'tab:blue'
    ax2.set_ylabel('Copper Accidents', color=color)  # we already handled the x-label with ax1
    ax2.plot(year_list, num_copper_accidents, color=color)
    ax2.tick_params(axis='y', labelcolor=color)

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.title("Total Accidents and Copper Mining Accidents in US, 2010-2019")
    plt.show()

    #Draw Graphs based on Day of the Week, Total Time out of Work
    print(days_lost_stats)

if __name__=="__main__":
    main()
