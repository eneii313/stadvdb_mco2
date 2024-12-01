window.onload = function () {
    var searchText = document.getElementById("searchText");
    var searchBtn = document.getElementById("searchBtn");

    // Disable button initially if the textbox is empty
    toggleButtonState();

    searchText.addEventListener("input", function () {
        toggleButtonState();
    });

    function toggleButtonState() {
        if (searchText.value.trim() === "") {
            searchBtn.disabled = true;
        } else {
            searchBtn.disabled = false;
        }
    }
};