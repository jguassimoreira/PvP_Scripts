#### Parents vs Peers Cross-Context Data Processing Script ####

## This script creates a pipeline for cleaning and processing
## behavioral data from the PvP_CC study. It does so by creating
## a set of functions for processing each individual task and then
## assigns them into a broader class that cleans all the subject's
## data when called.

import pandas as pd
import numpy as np
import glob
import inspect

### REFRESH AGGREGATE FILES BY CLEARING OLD DATA ###
#CCT
cct = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\CCT_Level1.csv")
cct.iloc[0:0].to_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\CCT_Level1.csv", index=False)

#WYR
wyr_prob_mon = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\WYR_Prob_Mon_Level1.csv")
wyr_prob_mon[0:0].to_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\WYR_Prob_Mon_Level1.csv", index=False)

wyr_prob_soc = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\WYR_Prob_Soc_Level1.csv")
wyr_prob_soc[0:0].to_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\WYR_Prob_Soc_Level1.csv", index=False)

wyr_temp_mon = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\WYR_Temp_Mon_Level1.csv")
wyr_temp_mon[0:0].to_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\WYR_Temp_Mon_Level1.csv", index=False)
 
wyr_temp_soc = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\WYR_Temp_Soc_Level1.csv")
wyr_temp_soc[0:0].to_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\WYR_Temp_Soc_Level1.csv", index=False)

#SELF & OTHER
selfother = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\selfOther.csv")
selfother[0:0].to_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\selfOther.csv", index=False)


### CLASS DEFINEMENT ###

#Define the class 'clean', which is going to clean all the subject's data
class clean:
    
    def __init__(self, name):
        self.name = name
    
    def clean_CCT(*args,**kwargs):
        print("Cleaning CCT")
        out = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\CCT_Level1.csv")
        for dir in taskDirs["CCT"]:
            data = pd.read_csv("%s"%(dir))
            subID = dir.split('\\')[-3]
            subdir = str(taskDirs["CCT"][0])
            dataProc = pd.concat([data.iloc[:,39], data.iloc[:,4], data.iloc[:,8], data.iloc[:,33], data.iloc[:,12], data.iloc[:,15]],  axis = 1)
            dataProc = dataProc.dropna()
            tn = np.arange(1,len(dataProc)+1)
            dataProc.insert(loc=1, column="trialNum", value=tn)
            dataProc = dataProc.rename(index=str, columns={"participant": "ID", "deckResp.corr": "Decision", "EV": "Return", "SD": "Risk"})
            outdir = '\\'.join(subdir.split("\\")[:-2])
            if dataProc.iloc[0,3] == 1:
                condLab = "ParentGain_FriendLose"
            else:
                condLab = "FriendGain_ParentLose"
            dataProc.to_csv("%s\\Clean\\%s_CCT_%s.csv"%(outdir, subID, condLab), index = False)
            out = out.append(dataProc)
            out.to_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\CCT_Level1.csv", index=False)
    # print(inspect.getargspec(clean_CCT))
                
    def clean_WYR(*args,**kwargs):
        print("Cleaning WYR")
        out_Prob_Mon = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\WYR_Prob_Mon_Level1.csv")
        out_Prob_Soc = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\WYR_Prob_Soc_Level1.csv")
        out_Temp_Mon = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\WYR_Temp_Mon_Level1.csv")
        out_Temp_Soc = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\WYR_Temp_Soc_Level1.csv")
        out_dict = {'Prob_Mon': out_Prob_Mon, 'Prob_Soc': out_Prob_Soc, 'Temp_Mon': out_Temp_Mon, 'Temp_Soc': out_Temp_Soc}

        for dir in taskDirs["WYR"]:
            data = pd.read_csv("%s"%(dir)).iloc[1:,:]
            subdir = str(taskDirs['WYR'][0])
            subID = dir.split('\\')[-3]
            taskid = dir.split("\\")[-1].split('WYR_')[1].split('.')[0].split('_Raw')[0]
            if taskid == 'Prob_Mon':
                dataProc = pd.concat([data.iloc[:,18], data.iloc[:,12], data.iloc[:,0].str.replace('%','').astype(float), data.iloc[:,4].str.replace('%','').astype(float), data.iloc[:,1].str.replace('$','').astype(float), data.iloc[:,5].str.replace('$','').astype(float)], axis = 1)
            elif taskid == 'Prob_Soc':
                dataProc = pd.concat([data.iloc[:,18], data.iloc[:,12], data.iloc[:,0].str.replace('%','').astype(float), data.iloc[:,4].str.replace('%','').astype(float), data.iloc[:,1].str.replace(' Minutes','').astype(float), data.iloc[:,5].str.replace(' Minutes','').astype(float)], axis = 1)
            elif taskid == "Temp_Mon":
                dataProc = pd.concat([data.iloc[:,18], data.iloc[:,12], data.iloc[:,2], data.iloc[:,0], data.iloc[:,1].str.replace('$','').astype(float), data.iloc[:,5].str.replace('$','').astype(float)], axis = 1)
            else:
                dataProc = pd.concat([data.iloc[:,18], data.iloc[:,12], data.iloc[:,2], data.iloc[:,0], data.iloc[:,1].str.replace(' Minutes','').astype(float), data.iloc[:,5].str.replace(' Minutes','').astype(float)], axis = 1)
            tn = np.arange(1,len(dataProc)+1)
            rewdRatio = dataProc.iloc[:,5]/ dataProc.iloc[:,4]
            cond = np.where(data['disID']=='Parent: ', 1, 0) 
            dataProc.insert(loc = 1, column = "trialNum", value = tn)
            dataProc.insert(loc = 2, column = "Condition", value = cond)
            dataProc.insert(loc = 4, column = "RewardRatio", value = rewdRatio)
            dataProc = dataProc.rename(index=str, columns={"participant": "ID", "choiceKey.keys": "Decision"})
            outdir = '\\'.join(subdir.split("\\")[:-2])
            dataProc.to_csv("%s\\Clean\\%s_WYR_%s.csv"%(outdir, subID, taskid), index = False)
            dataOut = dataProc.iloc[:,0:5]
            out = out_dict[taskid].append(dataOut)
            out.to_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\WYR_%s_Level1.csv"%(taskid), index = False)
    
    def clean_colorCard(*args,**kwargs):
        print("Cleaning colorCard")
        for dir in taskDirs["colorCard"]:
            data = pd.read_csv("%s"%(dir)).iloc[1:,:]
            subdir = str(taskDirs["colorCard"][0])
            subID = dir.split('\\')[-3]
            dataProc = pd.concat([data.iloc[:,18], data.iloc[:,13], data.iloc[:,9], data.iloc[:,17], data.iloc[:,30], data.iloc[:,34], data.iloc[:,35]], axis = 1)
            dataProc = dataProc.dropna()
            dataProc = dataProc.rename(index = str, columns = {"choiceKey.keys": "decRaw", "parentEarningsThisi": "pOut", "friendEarningsThisi": "fOut"})
            dec = np.where(dataProc['PgFg_Code']==dataProc['decRaw'], 1, 
                  np.where(dataProc['PlFl_Code']==dataProc['decRaw'], 2,
                  np.where(dataProc['PgFl_Code']==dataProc['decRaw'], 3,
                  np.where(dataProc['PlFg_Code']==dataProc['decRaw'], 4, 999))))
            for d in dec: assert d != 999, "Error. Invalid condition code. Do you have missing decisions or mismatched dataframe columns?"
            dataProc.insert(loc = 7, column = "dec", value = dec)
            outdir = '\\'.join(subdir.split("\\")[:-2])
            dataProc.to_csv("%s\\Clean\\%s_colorCard.csv"%(outdir, subID), index = False)
    
    def clean_selfOther(*args,**kwargs):
        print("Cleaning selfOther")
        out = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\selfOther.csv")
        valences = ["positive", "neutral", "negative"]
        agents = ["PARENT", "FRIEND"]
        simDict = {}

        for dir in taskDirs["selfOther"]:
            for agn in agents:
                corrs = []
                for val in valences:
                    data = pd.read_csv("%s"%(dir))
                    dat = data[data["valence"] == "%s"%(val)]
                    x = dat[dat["agentLab"] == "%s"%(agn)].sort_values("trait")["traitResp.keys"]; x = x.reset_index(drop = True)
                    y = dat[dat["agentLab"] == "SELF"].sort_values("trait")["traitResp.keys"]; y = y.reset_index(drop = True)
                    data = pd.DataFrame([x,y]); data = np.transpose(data)
                    data = data[data.iloc[:,:] != "None"].dropna()
                    corrs.append(np.corrcoef(data.iloc[:,0].astype(int), data.iloc[:,1].astype(int))[[1,0]])
                simDict[agn] = np.arctanh(np.average(corrs))
            subID = dir.split('\\')[-3]
            outDat = pd.DataFrame({'ID': subID, 'selfParentSim': simDict['PARENT'], 'selfFriendSim': simDict['FRIEND']}, index=[0])
            out = out.append(outDat)
            out.to_csv("P:\\Parents_vs_Peers_(PvP)\\crossContext_subjPool\\Data\\selfOther.csv", index=False)
    
    def clean_gambling(*args,**kwargs):
        print("Cleaning gambling")
        for dir in taskDirs["gambling"]:
            data = pd.read_csv("%s"%(dir)).iloc[1:,:]
            subdir = str(taskDirs['gambling'][0])
            subID = dir.split('\\')[-3]
            cond = dir.split("\\")[-1].split('Gambling_')[1].split('.')[0].split('_Raw')[0]
            #dataProc = pd.concat([data.iloc[:,3].str.replace('$','').astype(float), data.iloc[:,0].str.replace('$','').astype(float), data.iloc[:,4].str.replace('$','').astype(float), data.iloc[:,23]], axis = 1)
            dataProc = pd.concat([data.iloc[:,3].str.replace('$','').astype(float), data.iloc[:,0].str.replace('$','').str.replace('(','').str.replace(')','').astype(float), data.iloc[:,4].str.replace('$','').astype(float), data.iloc[:,23]], axis = 1)
            if dataProc['lossAmt'].sum() > 0:
                dataProc['lossAmt'] = dataProc.where(dataProc['lossAmt'] == 0, -dataProc).iloc[:,1]
            dataProc = dataProc.dropna()
            dataProc = dataProc.rename(index = str, columns ={"certainAmt": "certainAmount", "lossAmt": "lossAmount", "gainAmt": "gainAmount", "choiceKeys.keys": "dec"})
            outdir = '\\'.join(subdir.split("\\")[:-2])
            dataProc.to_csv("%s\\Clean\\%s_Gambling_%s.csv"%(outdir, subID, cond), index=False)
        
    def clean_doors(*args,**kwargs):
        print("Cleaning doors")
        for dir in taskDirs["doors"]:
            subdir = str(taskDirs['doors'][0])
            subID = dir.split('\\')[-3]
            data = pd.read_csv("%s"%(dir)).iloc[1:,:]
            taskid = dir.split("\\")[-1].split("Doors_")[1].split("_Raw")[0]
            dataProc = data.iloc[:,6:]
            dataProc = dataProc.iloc[:,:24]
            dataProc = dataProc.dropna()
            dataProc = dataProc.rename(index = str, columns={"decKeys.keys": "dec", "decKeys.rt": "RT"})
            outdir = '\\'.join(subdir.split("\\")[:-2])
            dataProc.to_csv("%s\\Clean\\%s_Doors_%s.csv"%(outdir, subID, taskid), index=False)
    
    def clean_all(self, *args, **kwargs):
        self.clean_CCT(*args, **kwargs)
        self.clean_WYR(*args, **kwargs)
        self.clean_colorCard(*args, **kwargs)
        self.clean_selfOther(*args, **kwargs)
        self.clean_gambling(*args, **kwargs)
        self.clean_doors(*args, **kwargs)
        


#Define subject IDs and list of tasks
#Can be adapted to include different tasks for future studies
#To do: Create subject list
#subID = "PP1000"
datadir = "P:\Parents_vs_Peers_(PvP)\crossContext_subjPool\Data\Behavioral_Data"
subList = glob.glob("%s\PP*"%(datadir))
taskList = ["CCT", "WYR", "colorCard", "selfOther", "gambling", "doors"]
taskDirs = {} #Initialize task list -- subject specific task directories will go here in just a bit

### CLEAN THE DATA BY DEFINING 'MAIN' FUNCTION ### 
def main():
    for sub in subList:

        ### SET UP DATA IDENTIFICATION PROCEDURES ###
        behavfiles = glob.glob("%s\%s\Raw\*.csv"%(datadir,sub.split('\\')[-1])) #Get behav files
        for task in taskList: taskDirs[task] = [] #populate taskDirs dict with labels
        for task in taskDirs: taskDirs[task] = glob.glob("%s\%s\Raw\*%s*.csv"%(datadir,sub.split('\\')[-1],task)) #populate taskDirs dict with behav directories
        print(taskDirs)

        ### LET USER KNOW THE DATA EACH SUBJECT HAS (FOR QA PURPOSES) ###
        for t, task in enumerate(taskDirs):
            if len(taskDirs[task]) == 0:
                print("There are no %s files"%(task))
                continue
            print(task)

        ### FINALLY CLEAN THE DATA
        cln = clean('cln')
        cln.clean_all(taskDirs)
        


### RUN THE SCRIPT ### 
if __name__ == "__main__":
  main()