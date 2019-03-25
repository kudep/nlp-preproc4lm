# %%
# %load_ext autoreload
# %autoreload 2

# %%
from deeppavlov import configs
from deeppavlov import build_model
# %%
# model = build_model(config=configs.generative_chit_chat.transformer_chit_chat)
model = build_model(config=configs.ner.ner_rus_lower_vpc_with_context)
model1 = build_model(config=configs.ner.ner_rus, download=True)
# model = build_model(config=configs.generative_chit_chat.transformer_chit_chat_100k)
# model = build_model(config=configs.generative_chit_chat.transformer_chit_chat)
model.load()
model1.load()
# %%
ners = [model,model1]
# %%
model(['Как дела у Дениса ?', 'Все хорошо, спасибо!', 'Что делаешь?'], [['Привет!', 'Добрый день!'], ['Ты как?'], []], [])

# %%
model1(['Как дела Vfif ?'])

# %%
data = [
        'Маша',
        'Маши',
        'Денис',
        'Дениса',
        'Пока',
        'Кора',
        'Ага',
        'Ты',
        'Кого',
        'Патрик',
]
# %%
[ner(data) for ner in ners]

# # %%
# agent = DefaultAgent([model])
# while True:
#     utterance = input("::")
#     print(agent([utterance]))