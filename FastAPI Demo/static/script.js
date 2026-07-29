document
    .getElementById("order")
    .addEventListener("click", function() {
        fetch("/order", {
            method: "POST"
        });
    });


document
    .getElementById("navigation")
    .addEventListener("click", function() {
        fetch("/navigation", {
            method: "POST"
        });
    });