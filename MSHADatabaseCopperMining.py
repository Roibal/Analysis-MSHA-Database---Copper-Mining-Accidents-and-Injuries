"""
Created February 12, 2021 by Joaquin Roibal for ME 489
Team Members: Braci Chester, Julienne Mamaita Essomba, Carlha Barreto
Updated: 4/23/2021

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
    body_rep = {100:"Head, NEC", 110: "Brain", 120: "Ears", 121: "Ears", 122: "Ears", 130: "Eyes", 140: "Face", 141: "Face",
                142: "Face", 143: "Face", 144: "Face", 150: "Scalp", 160: "Skull", 170: "Head", 200: "Neck", 300: "Upper Extremities",
                310: "Arm", 311: "Arm", 312: "Arm", 313: "Arm", 314: "Arm", 320: "Arm", 330: "Hand", 340: "Hand", 350: "Upper Extremities",
                400: "Trunk", 410: "Abdomen", 420: "Back", 430: "Chest", 440: "Hips", 450: "Shoulders", 460: "Trunk", 500: "Lower Extremities",
                510: "Leg", 511: "Thigh", 512: "Knee", 513: "Leg", 514: "Leg", 520: "Ankle", 530: "Foot", 540: "Toe", 550: "Lower Extremities",
                600: "Body Systems", 700: "Multiple Parts", 800: "Body Parts, NEC", 900: "Unclassified"}
    minemach_rep = {1: "Tramway", 2: "Air Compressor", 3: "Air transportation", 4: "Auger Machine", 5: "Power Shop Tools",
                    6: "Blow pipe", 7: "Boats", 8: "Bulldozer", 9: "Carriage mounted Drill", 10: "Chute", 11: "Cyclones", 12: "Continuous Miner",
                    13: "Conveyor", 14: "Crane", 15: "Crusher", 16: "Cutting Machine", 17: "Stone Cutting Machine",
                    18: "Dredge", 19: "Elevator/Skip", 20: "Electric Drill", 21: "Fan", 22: "Flotation (Mill)",
                    23: "Forklift", 24: "Front-end Loader", 25: "Gathering Arm Loader", 26: "Grizzlies", 27: "Shotcrete/Gunite",
                    28: "Non-Powered Handtools", 29: "Powered Handtools", 30: "Hoist", 31: "Hydraulic Jets", 32: "Impactor",
                    33: "Load-Haul-Dump", 34: "Locomotive", 35: "Longwall Machine", 36: "Longwall subparts",
                    37: "Mancar, mantrip", 38: "Man lift", 39: "Mill, Grinding", 40: "Milling Machine", 41: "Mine car, underground",
                    42: "Mine Car, Surface", 43: "Mucking Machine", 44: "Ore Haulage Truck, UG", 45: "Ore Haulage Trucks, Highway",
                    46: "Package Machine", 47: "Pneumatic Blasting Agent Loader", 48: "Pump", 49: "Raise borer", 50: "Raise Climber",
                    51: "Raw Coal Storage", 52: "Roadgrader", 53: "Rockdrill", 54: "Roof Bolting Machine", 55: "Rock Dusting Machine",
                    56: "Rotary Dump", 57: "Scraper Loader", 58: "Screen", 59: "Shortwall Machine", 60: "Dragline, Shovel, BWE",
                    61: "Shuttle Car", 62: "Skip Pocket", 63: "Slusher", 64: "Tamping Machine", 65: "Track Maintenance",
                    66: "Tractor [UG]", 67: "Trucks (Non Ore)", 68: "Tugger", 69: "Washers", 70: "Welding Machine", 71: "Machine, NEC"}

    state_count = []
    injury_count = []
    database_list = []
    bodypart_count = []
    minemach_count = []
    df_acc_list = []
    hand_list =[]
    back_list = []
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
        df_copper['PARTBODY'].replace(body_rep, inplace=True)
        df_copper['MINEMACH'].replace(minemach_rep, inplace=True)
        database_list.append(df_copper)
        hand_inj = df_copper[df_copper['PARTBODY']=="Hand"]     #Hand Injury was highest body part, further analysis
        back_inj = df_copper[df_copper['PARTBODY']=="Back"]     #Back Injury was second most common injury
        print("Number of Hand Injuries: ", len(hand_inj))
        print("Number of Back Injury: ", len(back_inj))
        hand_list.append(hand_inj)
        back_list.append(back_inj)
        df_state_counts = pd.Series(df_copper['STATE']).value_counts()      #Counting Injury Classification
        df_accident_counts = pd.Series(df_copper['ATYPE']).value_counts()
        df_body_counts = pd.Series(df_copper['PARTBODY']).value_counts()
        df_minemach_counts = pd.Series(df_copper['MINEMACH']).value_counts()
        state_count.append([year_list[i], df_state_counts])                 #Entering into list data format
        injury_count.append([year_list[i], df_accident_counts])
        bodypart_count.append([year_list[i], df_body_counts])
        minemach_count.append([year_list[i], df_minemach_counts])

        #fig, ax = plt.subplots()    #Comment in / out for single / multiple plots
        df_acc_list.append(df_accident_counts)
        print("YEAR: ", year_list[i])
        print(df_accident_counts)

        plt.scatter(df_copper["EXPTOTAL"], df_copper["DAYSLOST"], label=str(year_list[i]))
        days_lost_stats.append([year_list[i], df_copper['AGE'].describe()])


    plt.title("Days Lost and Total Mining Experience. Copper Mining in US, 2010-2019")
    plt.xlabel("Years Worked at a Mine")
    plt.ylabel("Days Lost")
    plt.legend()
    plt.show()

    for g, year in enumerate(bodypart_count):
        plt.plot(year[1][0:10], label=year_list[g], marker='o', linestyle='None')
    plt.title("Top 10 Yearly Injured Body Parts, US Copper Mining, 2010-2019")
    plt.xlabel("Injured Body Part")
    plt.ylabel("Number of Injuries")
    plt.legend()
    plt.show()

    #Create Additional Graphs focusing on two highest injured body part (Hand & Back)
    back_list_days =[]
    mean_back_age =[]
    mean_back_work =[]
    hand_list_days = []
    mean_hand_age = []
    mean_hand_work = []
    for i in range(0, 10):
        #Calculate total yearly days missed for back injury and hand injury
        back_list_days.append(back_list[i]["DAYSLOST"].sum())
        mean_back_age.append(back_list[i]["AGE"].mean())
        mean_back_work.append(back_list[i]["EXPTOTAL"].mean())
        hand_list_days.append(hand_list[i]["DAYSLOST"].sum())
        mean_hand_age.append(hand_list[i]["AGE"].mean())
        mean_hand_work.append(hand_list[i]["EXPTOTAL"].mean())
    print(mean_back_age)
    print(mean_hand_age)
    width = 0.35
    fig, ax = plt.subplots()

    Back1 = ax.bar(year_list, back_list_days, width, align='edge', label="Back Injury")
    Hand1 = ax.bar(year_list, hand_list_days, -0.35, align='edge', label="Hand Injury")
    ax.set_title("Comparing Total Days Lost Per Year Hand Injury v Back Injury, US Copper Mining, 2010-2019")
    ax.set_xlabel("Year")
    ax.set_ylabel("Total Days Lost")
    ax.set_xticks(year_list)
    ax.set_xticklabels(('2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019'))
    plt.legend()
    #plt.show()
    #autolabel(Back1, "left")
    #autolabel(Hand1, "right")

    fig.tight_layout()

    plt.show()


    plt.bar(year_list, mean_back_age, 0.35, align='edge', label="Back Injury")
    plt.bar(year_list, mean_hand_age, -0.35, align ='edge', label="Hand Injury")
    plt.title("Comparing Average Age Hand Injury v Back Injury, US Copper Mining, 2010-2019")
    plt.xlabel("Year")
    plt.ylabel("Average Age of Miner")
    plt.legend()
    plt.ylim(30,50)
    plt.show()

    plt.bar(year_list, mean_back_work, 0.35, align='edge', label="Back Injury")
    plt.bar(year_list, mean_hand_work, -0.35, align='edge', label="Hand Injury")
    plt.title("Comparing Average Work Experience Hand Injury v Back Injury, US Copper Mining, 2010-2019")
    plt.xlabel("Year")
    plt.ylabel("Average Years of Work Experience")
    plt.legend()
    plt.ylim(3,10)
    plt.show()

    """for g, year in enumerate(minemach_count):
        plt.hist(year[1][1:11], label=year_list[g], marker='o', linestyle='None')
    plt.title("Top 10 Yearly Mine Machine Injuries, US Copper Mining, 2010-2019")
    plt.xlabel("Mine Machine")
    plt.ylabel("Number of Injuries")
    plt.legend()
    plt.show()"""

    for i, x in enumerate(df_acc_list):
        plt.plot(x, label=str(year_list[i]), marker='o', linestyle='None')
    plt.title("Accident Type and Total Number, Copper Mining in US, 2010-2019")
    plt.xlabel("Accident Type")
    plt.ylabel("Number of Yearly Accidents")
    plt.legend()
    plt.show()

    #Need to remove NaN from partbody / dayslost
    """
    for i, z in enumerate(database_list):
        plt.plot(z["PARTBODY"], z["DAYSLOST"], label=str(year_list[i]))
    plt.title("Body Part Injury and Number of Days Lost, Copper Mining in US, 2010-2019")
    plt.xlabel("Body Part")
    plt.ylabel("Number of Days Lost Per Accident")
    plt.legend()
    plt.show()
    """


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

    #Create stacked histogram (bar chart) with age of mine worker and num accidents
    """prev=0
    for l, cop in enumerate(database_list):
        cop['AGE'].hist(label=str(year_list[i]), bottom=prev, linewidth=2) #Creating a Histogram of Miner Age
        prev+=cop['AGE'].value_counts()
    plt.title("Histogram of Injured Miner Ages in US Copper Mining Industry, 2010-2019")
    plt.xlabel("Age of Injured Miner")
    plt.ylabel("Number of Miners Injured")
    plt.legend()
    plt.grid(False)
    plt.show()"""

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

def autolabel(rects, xpos='center'):
    #From Matplotlib Example Gallery
    """
    Attach a text label above each bar in *rects*, displaying its height.

    *xpos* indicates which side to place the text w.r.t. the center of
    the bar. It can be one of the following {'center', 'right', 'left'}.
    """

    ha = {'center': 'center', 'right': 'left', 'left': 'right'}
    offset = {'center': 0, 'right': 1, 'left': -1}

    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(offset[xpos]*3, 3),  # use 3 points offset
                    textcoords="offset points",  # in both directions
                    ha=ha[xpos], va='bottom')


    #TO DO:
    #Output all graphs, data, etc to a file or multiple files

if __name__=="__main__":
    main()