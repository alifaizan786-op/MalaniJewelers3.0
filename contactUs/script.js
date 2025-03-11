// Basic form submission handling
document.querySelector("form").addEventListener("submit", function (e) {
    e.preventDefault();
    alert("Form submitted successfully!");
    this.reset();
});
