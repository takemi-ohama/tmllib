import pdb
import pandas as pd
import numpy as np
import sklearn.metrics as mt
from collections import Counter
from sklearn.model_selection import GridSearchCV
import traceback

class Evaluator:
    u"""
    トレーニング関連ライブラリ
    """
    
    def __init__(self, train_X=None, train_y=None, test_X=None, test_y=None):
        self.train_X, self.train_y, self.test_X, self.test_y = train_X, train_y, test_X, test_y
        self.clf = None;
    
    def show_importance(self, feature_importance, test_X, pred_y=None):
        we = pd.DataFrame(list(test_X.columns), columns = ["name"])
        we.set_index('name',inplace=True)
        we["importances"] = feature_importance
        we['type'] = test_X.dtypes.rename('type')
        
        #yに対する相関と型、カウントを追加
        test = test_X.copy()
        ty = test.dtypes.rename('type')
        weights = pd.concat([we,ty],axis=1,join='outer')

        print("list-----------------------------------")
        self.printfull(weights)
        
        print("Top&Worst------------------------------")
        weights.sort_values("importances", ascending = False, inplace = True)
        self.print_top20(weights)
        
    
    def show_importances(self, clf, test_X=None, test_y=None, pred_y=None):
        self.show_importance(clf.feature_importances_,test_X,pred_y)
        
    def printfull(self, x):
        max_rows = pd.get_option('display.max_rows')
        pd.set_option('display.max_rows', None)
        display(x)
        pd.set_option('display.max_rows', max_rows)

    def print_top20(self, x):
        min_rows = pd.options.display.min_rows
        pd.options.display.min_rows = 40
        display(x)
        pd.options.display.min_rows = min_rows
        
    def show_valuation(self, test_y, pred_y, prob_y = None):
        print("...valuation...")
        report = mt.classification_report(test_y, pred_y,output_dict=True)
        df = pd.DataFrame(report).T
        df['support'] = df['support'].astype('int')
        display(df)
        print('accuracy_score: {0:.3f}'.format( mt.accuracy_score(test_y, pred_y)))
        if prob_y is not None:
            try: 
                print('average_precision: {0:.3f}'.format( mt.average_precision_score(test_y, prob_y)))
                print('roc_auc: {0:.3f}'.format( mt.roc_auc_score(test_y, prob_y)))            
            except Exception as e:
                mes = traceback.format_exception_only(type(e), e)
                if 'multi' not in mes[0]:
                    raise e
        print('MCC: {0:.3f}'.format(mt.matthews_corrcoef(test_y, pred_y)))
            

    def show_roc(self, test_y, prob_y):
        roc_auc = mt.roc_auc_score(test_y, prob_y)
        fpr, tpr, thresholds = mt.roc_curve(test_y, prob_y)
        self.plot_roc(fpr, tpr, roc_auc)
        
    def plot_roc(self, fpr, tpr, roc_auc, title='ROC curve'):
        import matplotlib.pyplot as plt
        plt.rcParams['font.family'] = 'IPAGothic'
        plt.figure()
        lw = 2
        plt.plot(fpr, tpr, color='darkorange',
                 lw=lw, label=f'{title} (area = %0.2f)' % roc_auc)
        plt.plot([0, 1], [0, 1], color='navy', lw=lw, linestyle='--')
        plt.xlim([0.0, 1.0])
        plt.ylim([0.0, 1.05])
        plt.xlabel('False Positive Rate')
        plt.ylabel('True Positive Rate')
        plt.title(title)
        plt.legend(loc="lower right")
        plt.show()

    def show_roc_multiclass(self, test_ys, prob_ys):
        for name in prob_ys:
            test_y = test_ys[name]
            prob_y = prob_ys[name]
            roc_auc = mt.roc_auc_score(test_y, prob_y)
            fpr, tpr, thresholds = mt.roc_curve(test_y, prob_y)
            self.plot_roc(fpr, tpr, roc_auc, title=name)
            
        