import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

class calcDowntime:
    """ calculate the uptime for all accountIds
    """

    def __init__(self, df_route_users, df_heartbeat, df_acc):
        """ 
        df_route: the route section of log, with the index of time
        df_heartbeat: the heartbeat section of log, with the index of time
        df_acc: the processed dataframe, containing
            index: accountId
            colums: accountName, startTime, endTime, deltaTime, deltaTime_min
        """
        self.df_route_users = df_route_users
        self.df_heartbeat = df_heartbeat
        self.df_acc = df_acc
        self.accountIds = self.df_acc.index.values


    def calc_downtime_percentage(self):
        """ calculate and add downtime column to self.df_acc
        """
        dict_downtime = {}
        for userId in self.accountIds:
            print("processing accountId ", userId)

            cond_user = self.df_route_users['accountId'] == userId
            df_route_user = self.df_route_users[cond_user]

            # calculate approximate downtime - minutes
            downtime_app_minutes = self.calc_downtime_approximation(df_route_user, userId)

            # calculate the uptime compensation - seconds
            uptime_comp_seconds = self.calc_uptime_compensation(df_route_user, userId)

            downtime = downtime_app_minutes - (uptime_comp_seconds / 60.0)

            dict_downtime[userId] = downtime
            
        ds = pd.DataFrame(dict_downtime.items(), columns=['accountId', 'downtime_min'])
        ds = ds.set_index('accountId')

        self.df_acc = pd.concat([self.df_acc, ds], axis=1)

        self.df_acc['downtime (%)'] = self.df_acc['downtime_min'] / \
                                self.df_acc['deltaTime_min'] * 100

        self.df_acc['downtime (%)'] = self.df_acc['downtime (%)'].round(2)

        return self.df_acc

   
    def calc_downtime_approximation(self, df_route_user, userId): 
        """ Calculate approximate downtime 
        """
        # process routing data
        df_route_user_sel_s = df_route_user[['event', 'accountId', 'serverId']]

        # process heartbeat data
        df_heartbeat_sel_s = self.df_heartbeat[['serverId', 'workload']].\
                            rename(columns={'serverId':'serverId_hb', \
                            'workload':'workload_hb'})

        # timerange for userID
        timerange = pd.date_range(start=self.df_acc.loc[userId, 'startTime_app'], 
                    end=self.df_acc.loc[userId, 'endTime_app'], freq='min')

        # set timerange for userID to be index of the route_userID dataframe
        df_route_user_exp = df_route_user_sel_s.reindex(timerange, method='ffill')

        # combine the expanded route_userID with heartbeat 
        df_comb = pd.concat([df_route_user_exp, df_heartbeat_sel_s], axis=1, sort=False)

        # extract userID data - all time, route, server, workload
        df_user = df_comb.loc[ (df_comb['serverId'] == df_comb['serverId_hb'])]

        # calculate approximate downtime
        uptime = df_user[df_user['workload_hb'] <=1].shape[0]
        time_requested = self.df_acc.loc[userId, 'deltaTime_min'] 
        downtime_app_minutes = time_requested - uptime

        return downtime_app_minutes


    def calc_uptime_compensation(self, df_route_user, userId):
        """ Examine corner case: routing at non-exact minute 
        """

        df_route_user_sel = df_route_user[['event', 'accountId', 'time_accu', \
                                        'time_prev', 'time_post', \
                                        'serverId_from', 'serverId_to']]

        # process hearbeat data
        df_heartbeat_sel = self.df_heartbeat[['serverId', 'workload']]

        df_heartbeat_sel.loc[:, 'time_prev'] = df_heartbeat_sel.index
        df_heartbeat_sel.loc[:, 'time_post'] = df_heartbeat_sel.index
        df_heartbeat_sel.loc[:, 'serverId_from'] = df_heartbeat_sel.loc[:, 'serverId']
        df_heartbeat_sel.loc[:, 'serverId_to'] = df_heartbeat_sel.loc[:, 'serverId']

        df_heartbeat_sel = df_heartbeat_sel[['workload', 'time_prev', 'time_post', 'serverId_from', 'serverId_to']]

        # add workload for serverId_from at time_prev
        df_route_user_prev = pd.merge(df_route_user_sel, df_heartbeat_sel, \
                                        how='left',\
                                        on=['time_prev', 'serverId_from']) 

        cols_sel = ['event', 'accountId', 'time_accu', \
                    'time_prev', 'serverId_from', 'workload', \
                    'time_post_x', 'serverId_to_x']

        cols_rename = {'workload':'workload_from', \
                       'time_post_x':'time_post', \
                       'serverId_to_x':'serverId_to'}

        df_route_user_prev = df_route_user_prev[cols_sel].rename(columns=cols_rename)

        # add workload for serverId_to at time_to
        df_route_user_prevpost = pd.merge(df_route_user_prev, df_heartbeat_sel,\
                                        how='left',\
                                        on=['time_post', 'serverId_to'])

        cols_sel2 = ['event', 'accountId', 'time_accu', \
                    'time_prev_x', 'serverId_from_x', 'workload_from', \
                    'time_post', 'serverId_to', 'workload']

        cols_rename2 = {'workload':'workload_to', \
                        'time_prev_x':'time_prev', \
                        'serverId_from_x':'serverId_from'}

        df_route_user_prevpost = df_route_user_prevpost[cols_sel2].\
                                 rename(columns=cols_rename2)

        # find the cases when time_accu is non-exact minute
        condition_cc = df_route_user_prevpost['time_accu'].dt.second != 0
        df_route_user_cc = df_route_user_prevpost[condition_cc]

        # if serverId_from is na, ignore data
        df_route_user_cc = df_route_user_cc[df_route_user_cc['serverId_from'].notna()] 

        # Routing server_from is up and server_to is down, then 
        #        -> add the seconds before routing to total server uptime.
        # Routing server_from is down server_to is up, then
        #        -> deduct the seconds after routing from total server uptime.
        condition_1 = (df_route_user_cc['workload_from'] <= 1) | \
                      (df_route_user_cc['workload_to'] > 1)
        condition_2 = (df_route_user_cc['workload_from'] > 1) | \
                      (df_route_user_cc['workload_to'] <= 1)

        uptime_add_seconds = df_route_user_cc[condition_1].time_accu.dt.second
        uptime_deduct_seconds = 60 - df_route_user_cc[condition_2].time_accu.dt.second
        uptime_comp_seconds = sum(uptime_add_seconds) - sum(uptime_deduct_seconds)

        return uptime_comp_seconds