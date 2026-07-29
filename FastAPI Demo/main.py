from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi import Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

#FastAPI is 
app = FastAPI()


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


# When the user clicks the order or event buttons, we have embdedded a java script script to handle, fetch, and make a post to /order and /event.  
# Then these functions get called
@app.post("/order")
def log_order():
    print("Order event received")
    return {"message": "Order logged"}


@app.post("/navigation")
def log_navigation():
    print("Navigation event received")
    return {"message": "Navigation logged"}




