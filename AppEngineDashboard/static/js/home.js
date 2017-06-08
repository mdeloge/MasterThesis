/**
 * Created by stijn on 15.05.17.
 */

jQuery(document).ready(function ($) {
    $(".clickable-row").click(function () {
        window.location = $(this).data("href");
    });
});