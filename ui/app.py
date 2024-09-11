import streamlit as st
from openai import OpenAI
import sys, os, pandas
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import asyncio

if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

st.header("Client APP Chatbot")
from adapters.sql_adapter import SQLAdapter
OPENAI_API_KEY = "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"
st.session_state.sql_adapter = SQLAdapter(api_key=OPENAI_API_KEY)
current_user = st.sidebar.selectbox(
    "USER",
    options=["37001", "37008", "37031"]
)
def html_generator(table: pandas.DataFrame):
    client = OpenAI(api_key=OPENAI_API_KEY)
    res = client.chat.completions.create(model="gpt-4-turbo",
                                         messages=[{"role":"system", "content":"You are a helpful AI who can create a table HTML code from a given dictionary of data. The HTML should be a proper table with enough heading styles and values from the dictionaries. Do not add any additional model messages or instructions. There should not be any strings like ```html``` in the output. The string should be directly renderable."},
                                                   {"role":"user", "content":f"{table.to_dict()}"}],
                                                   temperature=0.5,
                                                   )
    return res.choices[0].message.content



def handle_input():
    query = st.session_state.query
    if query:
        try:
            st.session_state.chat_history.append(("User", query))

            response_table = st.session_state.sql_adapter.special_agent(user_query = f"The mandateId is {current_user}, looking for {query}")
            print(response_table)
            html_string = response_table.to_html()#html_generator(response_table)
            print(html_string)
            st.session_state.chat_history.append(("Bot", html_string))
            
        except Exception as e:
            print(e)
            st.session_state.chat_history.append(("Bot", f"Sorry. we couldn't find what you are looking for. Please rephrase the question with more information."))
        st.session_state.query = ""




for user, message in st.session_state.chat_history:
    if user == 'User':
            st.markdown(
                f'<p style="color: blue; background-color: #f0f0f0; padding: 10px; border-radius: 10px;">You: {message}</p>',
                unsafe_allow_html=True
            )
    elif message.startswith("Sorry"):
        st.markdown(
                f'<p style="color: blue; background-color: #f0f0f0; padding: 10px; border-radius: 10px;">You: {message}</p>',
                unsafe_allow_html=True
            )
    else:
        rows = len(pandas.read_html(message)[0])
        # Set height dynamically based on number of rows
        height = min((rows * 30) + 50, 500)
        st.components.v1.html(message, height=height, scrolling=True)        
st.text_input("Enter your query..", key="query", on_change= handle_input)

