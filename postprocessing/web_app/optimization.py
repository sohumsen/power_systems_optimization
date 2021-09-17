
from pyomo.environ import ConcreteModel, Set, Param, Var,  Constraint, Objective, minimize, NonNegativeReals, SolverFactory, value
import pandas as pd
import math

# Sets


def commoditiy_prices_init(model, process):
    return pd.Series(
        model.processed_dict_df["ResourceCost"]["Cost (Euro/MWh)"].values,
        index=model.processed_dict_df["ResourceCost"]["Resource"].values).to_dict()[process]


def process_unit_cost_init(model, process):
    return pd.Series(model.processed_dict_df["ProcessData"]["Unit Cost (Euro/MW/a)"].values, index=(
        model.processed_dict_df["ProcessData"].Process.values)).to_dict()[process]


def storage_efficiency_init(model, region):

    return dict(zip(model.processed_dict_df["StorageData"].Storage.values, model.processed_dict_df["StorageData"].Efficiency.values))["Battery"]


def storage_unit_cost_init(model, region):

    return pd.Series(model.processed_dict_df["StorageData"]["Unit Cost (Euro/MWh/a)"].values, index=(
        model.processed_dict_df["StorageData"].Storage.values)).to_dict()["Battery"]


def efficiency_ratio_init(model, process):
    return pd.Series(model.processed_dict_df["ProcessData"]["Efficiency"].values, index=(
        model.processed_dict_df["ProcessData"].Process.values)).to_dict()[process]


def process_cap_up_init(model, process):
    return pd.Series(model.processed_dict_df["ProcessData"]["Maximum Capacity (MW)"].values,
                     index=(model.processed_dict_df["ProcessData"].Process.values)).to_dict()[process]


def average_demand_region_init(model, region):
    col = region
    return model.processed_dict_df["Demand"][col].mean()

# for processes other than solar and wind, the capacity factor ratio is 1. Solar and wind have time series data


def supim_capacity_factor_ratio_init(model, time, regions, process):

    if process in model.TimeSeriesProcs:

        col = regions+"."+process+".CapacityFactorRatio"
        row = int(float(time))
        return model.processed_dict_df["SupIm"].at[row, col]
    else:
        return 1


def demand_region_init(model, time, regions):

    col = regions
    row = int(float(time))
    return model.processed_dict_df["Demand"].at[row, col]


def gwp_process_init(model, process):
    return pd.Series(model.processed_dict_df["ProcessData"]["GWP (g/MJ)"].values, index=(
        model.processed_dict_df["ProcessData"].Process.values)).to_dict()[process]

# Establishing maximum capacity of energy storage given by the maximum of the difference between demand and supply in the time series


def max_storage_capacity_init(model, region):
    lst = []
    for time in range(len(model.Time)):
        tot = 0
        for process in model.Processes:

            tot += (model.average_demand_region[region]*(model.region_fraction[region,
                                                                               process].value * model.supim_capacity_factor_ratio[time, region, process]))

        lst.append((model.demand_region[time, region]-tot))
    return max(lst)


class TransitionModel(ConcreteModel):

    def __init__(self, processed_dict_df):
        super(TransitionModel, self).__init__()
        # the input data
        self.processed_dict_df = processed_dict_df
        # helper attrs
        self.region_processes = list(
            processed_dict_df["SupIm"].columns.values)[1:]

        self.HOUR_TO_SECONDS = 60*60
        self.TONNE_TO_GRAM = 10**6
        self.CO2_PRICE = processed_dict_df["PollutantCost"]["Cost (Euro/t)"][0]

    def setup(self):
        ''' Sets , Parameters, Vars, Constrains, Objective'''
        # Sets
        l_regions = {x.replace('.Elec', '')
                     for x in list(self.processed_dict_df["Demand"].columns.values)[1:]}

        l_processes = list(
            self.processed_dict_df["ProcessData"]['Process'].unique())
        l_time = list(self.processed_dict_df["SupIm"]['t'])
        l_tsprocs = list(self.processed_dict_df["TSProcess"]["Process"])
        l_renewableprocs = list(
            self.processed_dict_df["ResourceCost"].loc[self.processed_dict_df["ResourceCost"]['Cost (Euro/MWh)'] == 0]["Resource"])
        # Init Sets
        self.Regions = Set(initialize=list(l_regions))
        self.Processes = Set(initialize=list(l_processes))
        self.Time = Set(initialize=list(l_time))
        self.TimeSeriesProcs = Set(initialize=list(l_tsprocs))
        self.RENEWABLE_PROCESSES = Set(initialize=list(l_renewableprocs))

        # Add more columns to the data
        self.enrich_data()

        # Parameters
        self.commoditiy_prices = Param(
            self.Processes, initialize=commoditiy_prices_init)

        self.process_unit_cost = Param(
            self.Processes, initialize=process_unit_cost_init)

        self.storage_efficiency = Param(
            self.Regions, initialize=storage_efficiency_init)
        self.storage_unit_cost = Param(
            self.Regions, initialize=storage_unit_cost_init)

        self.efficiency_ratio = Param(
            self.Processes, initialize=efficiency_ratio_init)

        self.process_cap_up = Param(
            self.Processes, initialize=process_cap_up_init)

        self.average_demand_region = Param(
            self.Regions, initialize=average_demand_region_init)

        self.supim_capacity_factor_ratio = Param(self.Time, self.Regions, self.Processes,
                                                 initialize=supim_capacity_factor_ratio_init)

        self.demand_region = Param(
            self.Time, self.Regions, initialize=demand_region_init)

        self.process_adjust = Param(
            self.Regions, self.Processes, domain=NonNegativeReals, initialize=1)
        self.gwp_process = Param(self.Processes, initialize=gwp_process_init)

        # Variables
        self.region_fraction = Var(
            self.Regions, self.Processes, domain=NonNegativeReals, initialize=0.01)

        # Constraint/Rules

        self.region_fraction_sum = Constraint(
            self.Regions, rule=self.region_fraction_sum_rule)
        self.demand_region_less_than_cap_up = Constraint(
            self.Regions, self.Processes, rule=self.demand_region_lt_cap_up_rule)
        self.deficit_sum = Constraint(self.Regions, rule=self.deficit_sum_rule)

        self.max_storage_capacity = Param(
            self.Regions, initialize=max_storage_capacity_init)

        # Finally the Objective
        self.obj = Objective(rule=self.objective_function, sense=minimize)

    def enrich_data(self):
        self._add_capacity_col_to_SupIm()
        self._add_unit_cost_col_to_Process()
        self._add_mw_col_to_Process()
        self._add_unit_cost_col_to_Storage()

    def _fill_up_capacity(self, row, col_name):

        if ("Wind" in col_name):

            if 4 < row[col_name] <= 25:
                cell = row[col_name]

                return (math.exp(-((4/12)**2.25))-math.exp(-((cell/12)**2.25)))/((cell/12)**2.25-(4/12)**2.25)-(math.exp(-((25/12)**2.25)))
            else:
                return 0

        else:
            cell = row[col_name]
            if cell == 0:
                return 0
            else:
                return 0.025+(0.21-0.025)/((self.processed_dict_df["SupIm"])[col_name].max()-1)*(cell-1)

    def _add_capacity_col_to_SupIm(self):

        for i in range(len(self.region_processes)):
            norm_name = self.region_processes[i]
            self.processed_dict_df["SupIm"][norm_name+".Capacity"] = self.processed_dict_df["SupIm"].apply(
                lambda x: self._fill_up_capacity(x, norm_name),  axis=1)

    def _add_unit_cost_col_to_Process(self):

        df = self.processed_dict_df["ProcessData"]

        df['Unit Cost (Euro/MW/a)'] = df["Variable Cost (Euro/MW/a)"] + df["Investment Cost (Euro/MW)"]*(df["WACC"] * (1+df["WACC"])**df["Life  (years)"])/(
            (1+df["WACC"])**df["Life  (years)"]-1) + df["Fixed Cost (Euro/MW/a)"]

    def _add_unit_cost_col_to_Storage(self):

        df = self.processed_dict_df["StorageData"]

        df['Unit Cost (Euro/MWh/a)'] = df["Variable Cost (Euro/MWh/a)"] + df["Investment Cost (Euro/MWh)"]*(df["WACC"] * (1+df["WACC"])**df["Life  (years)"])/(
            (1+df["WACC"])**df["Life  (years)"]-1) + df["Fixed Cost (Euro/MWh/a)"]

    def _add_mw_col_to_Process(self):
        '''Wind or solar (time series data) power output from capacity factor'''
        SupIm_df = self.processed_dict_df["SupIm"]
        for i in range(len(self.region_processes)):
            norm_name = self.region_processes[i]

            if ("Wind" in norm_name):

                SupIm_df[norm_name+".CellPower"] = 0.6*8000 * \
                    SupIm_df[norm_name+".Capacity"] * \
                    SupIm_df[norm_name]**3/10**6

                SupIm_df[norm_name+".CapacityFactorRatio"] = SupIm_df[norm_name +
                                                                      ".CellPower"]/SupIm_df[norm_name+".CellPower"].mean()
            else:
                SupIm_df[norm_name+".CapacityFactorRatio"] = SupIm_df[norm_name +
                                                                      ".Capacity"] / SupIm_df[norm_name + ".Capacity"].mean()

    def region_fraction_sum_rule(self, model, region):

        s = 0.0
        for process in self.Processes:
            s += self.region_fraction[region, process]

        return s == 1
# the sum of the balance between the demand and the supply over the time series should be close to 0 so that the energy mix can meet the total demand

    def demand_region_lt_cap_up_rule(self, model, region, process):
        return self.region_fraction[region, process]*self.average_demand_region[region] <= self.process_cap_up[process]

    def deficit_sum_rule(self, model, region):
        deficit_sum = 0

        for time in model.Time:
            deficit_sum += model.demand_region[time, region]
            for process in model.Processes:

                deficit_sum += -model.region_fraction[region, process]*model.average_demand_region[region] * model.supim_capacity_factor_ratio[time,
                                                                                                                                               region, process]

        return deficit_sum <= 0

# """
#     It is taking the sum of the products of proportion of the process or storage type and their costs.
#     Thus, the objective function is the minimisation of the total cost.
#     It includes resource and pollutant (e.g. CO2) cost alongside fixed, variables and weighted average capital costs.
# """
    def objective_function(self, model):

        tot = 0
        for region in model.Regions:
            tot += (model.storage_unit_cost[region]/model.storage_efficiency[region]
                    * model.max_storage_capacity[region])
            for process in model.Processes:

                tot += model.region_fraction[region, process] * model.average_demand_region[region] * (model.process_unit_cost[process] + (model.commoditiy_prices[process] / model.efficiency_ratio[process]+model.gwp_process[process]*model.HOUR_TO_SECONDS/model.TONNE_TO_GRAM*model.CO2_PRICE)*len(
                    model.Time)) * model.process_adjust[region, process]

        return tot

    def optimize(self):

        # This assumes a processed perfect df
        self.setup()

        opt = SolverFactory("ipopt")
        # ipopt is non linear

        results = opt.solve(self, tee=False)

        return self, results
