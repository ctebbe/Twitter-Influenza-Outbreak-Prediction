# -*- coding: utf-8 -*-
"""
Created on Fri Nov 20 04:07:35 2015
@author: Johnson Charles Kachikaran Arulswamy
"""
# to get rid of - no display name and no $DISPLAY environment variable error
import matplotlib
matplotlib.use('Agg')

import csv
import matplotlib.pyplot as plt
import time
import numpy as np
from sklearn import linear_model
from sklearn.metrics import mean_squared_error
from math import sqrt
from prettytable import PrettyTable

X = []
R = []
# Function for Fitting our data to Linear model
def linear_model_main(X_parameters,Y_parameters,predict_value=None):
    # Create linear regression object which will compute the least squares for us
    regr = linear_model.LinearRegression()
    regr.fit(X_parameters, Y_parameters)
    predict_outcome = regr.predict(predict_value)
    predictions = {}
    predictions['intercept'] = regr.intercept_
    predictions['coefficient'] = regr.coef_
    predictions['predicted_value'] = predict_outcome
    return predictions
    
def train_model(maxM=5, maxN=5):
    lrmse = 100 #holds the least rmse value which is a percent value
    #training data lists    
    ilix = []
    iliy = []
    tweets = []
    years = []
    weeks = []
    #testing data lists
    tilix = []
    tiliy = []
    ttweets = []
    tweeks = []
    with open('ilitrain.csv', 'rb') as csvfile:
        ilireader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(ilireader, None) #skipping the header
        for row in ilireader:
            years.append(int(row[0]))
            weeks.append(int(row[1]))
            ilix.append(int(row[2]))
            iliy.append(int(row[2])*100000/int(row[3])) #ili patients per 100k visitors
            tweets.append(int(row[3]))
    
    with open('ilitest.csv', 'rb') as csvfile:
        ilireader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in ilireader:
            tweeks.append(int(row[1]))
            tilix.append(int(row[2]))
            tiliy.append(int(row[2])*100000/int(row[3]))
            ttweets.append(int(row[3]))
            
    with open('trainout.csv', 'wb') as csvfile:
        trainwriter = csv.writer(csvfile)
        trainwriter.writerow(["YEAR", "WEEK", "TWEETS", "ILICASES", "ILIPER100K"])
        for i in range(len(years)):
            trainwriter.writerow([years[i], weeks[i], tweets[i], ilix[i], iliy[i]])
        
    for m in range(maxM):
        for n in range(maxN):
            if m==0 and n==0:
                continue
            x = []
            y = []
            tx = []
            ty = []
            tw = []
            init = max(m, n-1) + 1 if n > 0 else m + 1

            for i in range(init, len(tweets) + 1):
                xdash = tweets[i-n:i]
                xdash.extend(ilix[i-m-1:i-1])
                x.append(xdash)
                y.append(iliy[i-1])
            
            for i in range(init, len(ttweets) + 1):
                xdash = ttweets[i-n:i]
                xdash.extend(tilix[i-m-1:i-1])
                tx.append(xdash)
                ty.append(tiliy[i-1])
                tw.append(tweeks[i-1])

            p = linear_model_main(x, y, tx)
            rmse = sqrt(mean_squared_error(np.array(ty), p['predicted_value']))
            X.append([(m, n), rmse, p['coefficient'].tolist(), p['predicted_value'].tolist()])
            if rmse/1000 < lrmse:
                lrmse = rmse/1000
                R = [(m, n), lrmse, p['coefficient'].tolist(), tw, map(lambda x: float(x)/1000, ty), map(lambda x: float(x)/1000, p['predicted_value'].tolist())]
    
    with open('log.csv', 'wb') as csvfile:
        logwriter = csv.writer(csvfile)
        logwriter.writerow(["(M, N)", "RMSE", "[Coefficients]", "[Predicted Values]"])
        for i in range(len(X)):
            logwriter.writerow(X[i])
        logwriter.writerow(["(M, N)", "Least RMSE %", "[Coefficients]", "[Week numbers]", "[Actual values]", "[Predicetd values]"])
        logwriter.writerow(R)
        
    fig = plt.figure(figsize=(16,10))
    ax = fig.add_subplot(1,1,1)                                                      
    # major ticks every 1, minor ticks every 0.2                                      
    y_major_ticks = np.arange(0, 7, 1)                                              
    y_minor_ticks = np.arange(0, 7, 0.2)                                               
    # major ticks every 4, minor ticks every 1    
    x_major_ticks = np.arange(0, 40, 4)
    x_minor_ticks = np.arange(0, 40, 1)
    ax.set_xticks(x_major_ticks)                                                       
    ax.set_xticks(x_minor_ticks, minor=True)                                           
    ax.set_yticks(y_major_ticks)                                                       
    ax.set_yticks(y_minor_ticks, minor=True)
    # differnet settings for the grids
    ax.grid(which='both', alpha=0.5)                                                
    plt.title("ILI Prediction: Actual vs Predicted Percent Values", fontsize = 20)
    plt.xlabel("Week Number", fontsize=18)
    plt.ylabel("Percentage of ILI Cases", fontsize=18)
    colors = ['m', 'b']
    labels = ['Predicted ILI', 'Actual ILI']
    plt.plot(R[3], R[5], color=colors[0], label=labels[0], linewidth=3,  marker='o')
    plt.plot(R[3], R[4], color=colors[1], label=labels[1], linewidth=3,  marker='s')
    plt.xticks(R[3])
    plt.legend(loc='upper center', ncol=2, fancybox=False, fontsize='xx-large')
    plt.savefig('ili_prediction_graph.png')
    plt.show()
    
    #showing m,n and the corresponding rmse values in a tabular format
    # and highlighting the least rmse
    print "\033[31;1m### m,n and rmse values (least rmse is highlighted) ###\033[0m"
    lm, ln = R[0]
    header = ["(m, n)"]
    header.extend(map(lambda x: "n = %s" %(x), [i for i in range(maxN)]))
    t = PrettyTable(header)
    for m in range(maxM):
        row = ["m = %s" %(m)]
        for n in range(maxN):
            if m == 0 and n == 0:
                row.append(" -- ")
                continue
            if m == lm and n == ln:
                #%.4f will print 4 decimals, \033[35;1m] magenta font bold set, \033[0m] no styling
                row.append("\033[35;1m%.4f\033[0m" %(X[m*maxN + n -1][1]/1000))
            else: row.append("%.4f" %(X[m*maxN + n -1][1]/1000))
        t.add_row(row)
    print t    
    
    #Displaying least RMSE and the corresponding m, n along with the coefficients
    print "\033[31;1m### (m,n) values, least RMSE\033[0m"
    print R[0], R[1]
    
    print "\033[31;1m### Least rmse coefficients\033[0m"
    print R[2]        

if __name__ == '__main__':
    start = time.time()
    train_model()
    end = time.time()
    print "\033[31;1m### Elapsed time:\033[0m %s seconds" %(end - start)