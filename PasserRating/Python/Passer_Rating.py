import os
class passerdata:
    def __init__(self):
        self.attempts=0
        self.completions=0
        self.yards=0
        self.touchdowns=0
        self.interceptions=0
        self.rating=0
        self.task=""
        self.completionpercent=0
        self.yardsperattempt=0
        self.tdsperattempt=0
        self.intperattempt=0
        self.passerating=0
   
    def fixvalue(self, valuein):
        MAXVALUE = 2.375
        MINVALUE = 0
        if valuein >= MAXVALUE:
            return MAXVALUE
        elif valuein <= MINVALUE:
            return MINVALUE
        else:
            return valuein
        
    def parseinput(self):
        if self.task == 'i':
            self.interceptions = int(input("Enter the number of interceptions: "))
        elif self.task == 't':
            self.touchdowns = int(input("Enter the number of touchdowns: "))
        elif self.task == 'y':
            self.yards=int(input("Enter the total yards: ")) 
        elif self.task == 'a':
            self.attempts=int(input("Enter the number of attempts: "))
        elif self.task == 'c':
            self.completions=int(input("Enter the number of completions: ")) 
        elif self.task == 'p':
            self.calccompletionpercent()
            self.calcyardpercatch()
            self.calctdperattempt()
            self.calcintperattempt()
            self.calcpasserratting()
        elif self.task =='q':
            exit()

    def validateinput(self):
        valid=(self.task == 'c' or self.task == 'a' or self.task == 'y' or self.task == 't' or self.task == 'i' or self.task == 'q' or self.task == 'p')
        if valid==False:
           wait = input("Invalid input: " + self.task+"\nPress ENTER to continue")
        if (self.task == 'p' and self.attempts==0):
           wait = input("Invalid input, rating cannnot be calculated if the number of attempts is zero\nPress ENTER to continue")
    
    def writedisplay(self):
        os.system('cls')
        if self.attempts==0:
            print("Attempts: "+str(self.attempts)+"\nCompletions: "+str(self.completions)+"\nYards: "+str(self.yards)+"\nTouchdowns: "+str(self.touchdowns)+
            "\nInterceptions: "+str(self.interceptions) + "\n\npasser rating: undefined")
        else:
            print("Attempts: "+str(self.attempts)+"\nCompletions: "+str(self.completions)+"\nYards: "+str(self.yards)+"\nTouchdowns: "+str(self.touchdowns)+
            "\nInterceptions: "+str(self.interceptions) + "\n\npasser rating: "+ str(self.passerating))
        self.task=input("\nType the first letter of a value to change that value (for example \"t\" for touchdowns)." 
            "\nType \"q\" to quit and \"p\" to calculate the passer rating: ")
    
    def calccompletionpercent(self):
        if(self.attempts==0):
            self.completionpercent=0
        else:
            temp=(self.completions/self.attempts-.3)*5
            self.completionpercent = self.fixvalue(temp)

    def calcyardpercatch(self):
        if(self.attempts==0):
            self.yardsperattempt=0
        else:
            temp=(self.yards/self.attempts-3)*.25
            self.yardsperattempt = self.fixvalue(temp)

    def calctdperattempt(self):
        if(self.attempts==0):
            self.tdsperattempt=0
        else:
            temp=(self.touchdowns/self.attempts)*20
            self.tdsperattempt=self.fixvalue(temp)

    def calcintperattempt(self):
        if(self.attempts==0):
            self.intperattempt=0
        else:
            temp=2.375-(self.interceptions/self.attempts * 25)
            self.intperattempt=self.fixvalue(temp)

    def calcpasserratting(self):
        self.calccompletionpercent()
        self.calcyardpercatch()
        self.calctdperattempt()
        self.calcintperattempt()
        self.passerating = ((self.completionpercent+self.yardsperattempt+self.tdsperattempt+self.intperattempt)/6)*100
        
    

        
pr=passerdata()
while pr.task != "q":
    pr.writedisplay()
    pr.validateinput()
    pr.parseinput()
    pr.calcpasserratting()

