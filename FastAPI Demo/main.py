from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi import Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from confluent_kafka import Producer
from datetime import datetime, timezone
import json

# User on the web hits a button, triggers javascript, it makes a request, ends up in uvicorn, uvicorn transmits that down to fast api, and we use fast api to 
# actually make our code here.  This is our app, fast api starts.  
app = FastAPI()


# We will need kafka producers for this assignment.  Default port on local machine for now.  
config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(config) # This producer can publish messages.  
# Messages are basically events.  Eg when a user clicks a button.  
# We will 



# We are going to append some javascript button functioinality to our page.  
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse) # When FASTAPI receives a get request from uvicorn / the user's request on the browser at the /home endpoint, run this function:  
def home(request: Request): # uvicorn sends the request that the user made in case we need its data.  
    return """
    <html>
    <body>

    <h1>Event System</h1>

    <button id="order">
        Order Event
    </button>

    <button id="navigation">
        Navigation Event
    </button>

    <script src="/static/script.js"></script>

    </body>
    </html>
    """

# Both get and post are "requests".  These are just different parameters of that http request.  
# A response is what we return once we handle the request.  


# We should probably use jinja 2 instead of sending raw html in the response.  
# We are also not using the request right now.  

# Redirect to "actual" home page
@app.get("/home")
def dir_to_home():
    return RedirectResponse("/")



# A get to /orders and a post to /orders are different endpoints.  We can define both.  
# Normal behaviour when a user visits a page is a get endpoint.  Posts are intentional generally.  

# Eg our javascript might have 
# fetch("/order", {
#     method: "POST"
# })
# This is a way we might trigger a post to /order

# A button, triggering JavaScript, causes a POST request to be made, Uvicorn --> FastAPI receives it, and we handle it with our Python code.


# "Each button should send a post request to a fastAPI backend that logs the event message to the console."  



def publish_event(event_type: str):

    event = {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    print(event)

    producer.produce(
        topic="events",
        value=json.dumps(event)
    )

    # For debugging purposes, we are temporarily making kafka synchronous.  
    producer.flush()


# When the user clicks the order or event buttons, we have embdedded a java script script to handle, fetch, and make a post to /order and /event.  
# Then these functions get called
@app.post("/order")
def log_order():

    #Create a kafka message
    publish_event("order")

    print("Order event received")
    return {"message": "Order logged"}


@app.post("/navigation")
def log_navigation():

    #Create a kafka message
    publish_event("navigation")

    print("Navigation event received")
    return {"message": "Navigation logged"}




