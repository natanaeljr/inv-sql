from datetime import *


def tarifas_b3(date_):
    if date_ < datetime.strptime("2021-02-02", '%Y-%m-%d').date():
        taxa_negociacao = 0.00003006
        taxa_liquidacao = 0.000275
    else:
        taxa_negociacao = 0.00005
        taxa_liquidacao = 0.00025
    return taxa_negociacao + taxa_liquidacao


def tarifas_corretora(broker, date_):
    if broker == 'rico':
        return tarifas_rico(date_)
    elif broker == 'clear':
        return tarifas_clear(date_)
    elif broker == 'itau':
        return 0  # todo
    else:
        raise Exception(f"Unsupported Broker: {broker}")


def tarifas_rico(date_):
    if date_ < datetime.strptime("2020-07-14", '%Y-%m-%d').date():
        taxa_corretagem = 7.5
        taxa_impostos_corretagem = 0.106666667  # PIS(1.65%) + COFINS(7.6%) + ISS(1.4166667%)??
    else:
        taxa_corretagem = 0
        taxa_impostos_corretagem = 0.106666667  # PIS(1.65%) + COFINS(7.6%) + ISS(1.4166667%)??
    return taxa_corretagem + (taxa_corretagem * taxa_impostos_corretagem)


def tarifas_clear(_date):
    return 0
