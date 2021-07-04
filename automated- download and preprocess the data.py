import pandas as pd

def newstart():

    z= pd.read_csv('https://data.ca.gov/dataset/f40b9c5c-ef29-4705-9f12-8325f4fb8fc5/resource/235466b6-0eb9-4ff7-a4b4-8138f474ce83/download/homeless_impact.csv')
    w=pd.read_csv('homeless2.csv')
    c=z.tail(len(z)-len(w))

    print(len(z))
    print(len(w))

    if c.empty:
        exit()
    else:
        x=c.iloc[0,c.columns.get_loc('date')]

        y=w.iloc[(len(w)-1),w.columns.get_loc('date')]
        dowork(c,x,y)

def dowork(c,x,y):

    if x!=y:
        dc=c.fillna(0)
        dc['rooms']=dc['rooms'].astype(int)
        dc['rooms_occupied']=dc['rooms_occupied'].astype(int)
        dc['trailers_requested']=dc['trailers_requested'].astype(int)
        dc['trailers_delivered']=dc['trailers_delivered'].astype(int)
        dc['donated_trailers_delivered']=dc['donated_trailers_delivered'].astype(int)
        dc['county']=dc['county'].apply(lambda x: x.replace('County'," "))
        dc.to_csv('homeless2.csv',header=False,index=False,mode='a')
        print("hello")
    else:
        print("bye")
        exit()
newstart()

# schedule.every(1).minute.do(newstart)
# while 1:
#     schedule.run_pending()
#     time.sleep(1)
