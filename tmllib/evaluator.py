import pandas as pd
import sklearn.metrics as mt
import traceback


class Evaluator:
    u"""
    トレーニング関連ライブラリ
    """

    def __init__(self, train_X=None, train_y=None, test_X=None, test_y=None):
        self.train_X, self.train_y, self.test_X, self.test_y = train_X, train_y, test_X, test_y
        self.clf = None;

    def show_importance(self, feature_importance, test_X, pred_y=None, num=20):
        # 特徴量名と重要度を含むデータフレームを作成
        feature_info = pd.DataFrame({'name': test_X.columns, 'importances': feature_importance})
        feature_info.set_index('name', inplace=True)
        feature_info['type'] = test_X.dtypes

        # 重要度が0でない項目を抽出
        non_zero_importance = feature_info[feature_info['importances'] != 0].copy()
        zero_importance = feature_info[feature_info['importances'] == 0].copy()

        # 特徴量の詳細情報を表示
        print("重要度リスト-----------------------------------")
        # self.printfull(non_zero_importance)

        # 重要度が0の項目を表示
        print("重要度0の特徴量---------------------------------")
        # self.printfull(zero_importance)

        # 重要度に基づいてトップとワーストの特徴量を表示
        print(f"トップ{num}------------------------------")
        non_zero_importance.sort_values("importances", ascending=False, inplace=True)
        self.print_topx(non_zero_importance, num)

    def show_importances(self, clf, test_X=None, test_y=None, pred_y=None):
        self.show_importance(clf.feature_importances_, test_X, pred_y)

    def printfull(self, x):
        max_rows = pd.get_option('display.max_rows')
        pd.set_option('display.max_rows', None)
        display(x)
        pd.set_option('display.max_rows', max_rows)

    def print_topx(self, x, num=20):
        with pd.option_context('display.max_rows', num * 2):
            display(x.head(num))

    def show_valuation(self, test_y, pred_y, prob_y=None):
        print("...valuation...")
        report = mt.classification_report(test_y, pred_y, output_dict=True)
        df = pd.DataFrame(report).T
        df['support'] = df['support'].astype('int')
        display(df)
        print('accuracy_score: {0:.3f}'.format(mt.accuracy_score(test_y, pred_y)))
        if prob_y is not None:
            try:
                print('average_precision: {0:.3f}'.format(mt.average_precision_score(test_y, prob_y)))
                print('roc_auc: {0:.3f}'.format(mt.roc_auc_score(test_y, prob_y)))
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
        plt.rcParams['font.family'] = 'IPAexGothic'
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
