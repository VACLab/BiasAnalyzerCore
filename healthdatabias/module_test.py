from healthdatabias.api import BIAS


if __name__ == '__main__':
    bias = BIAS()
    bias.set_config('/home/hongyi/HealthDataBias/config.yaml')
    bias.set_root_omop()
