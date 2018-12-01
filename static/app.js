$(function() {
    $('#upload-file-btn').click(function() {
        var form_data = new FormData($('#upload-file')[0]);
        $.ajax({
            type: 'POST',
            url: '/processor/upload_file',
            data: form_data,
            contentType: false,
            cache: false,
            processData: false,
            success: function(data) {
                console.log('Success!');
            },
            error: function(err){
                console.log(err);
                alert("error");
            }
        });
    });

    $('#upload-image-btn').click(function() {
        var form_data = new FormData($('#upload-image')[0]);
        $.ajax({
            type: 'POST',
            url: '/processor/image_upload',
            data: form_data,
            contentType: false,
            cache: false,
            processData: false,
            success: function(data) {
                console.log('Success!');
            },
            error: function(err){
                console.log(err);
                alert("error");
            }
        });
    });
    
    $('#broadcast-btn').click(function() {
        // var form_data = new FormData($('#broadcast-form')[0]);
        var data = {};
        $("#broadcast-form").serializeArray().forEach(function(obj){data[obj.name] = obj.value});
        data["mediaUrl"] = "";
        $.ajax({
            type: 'POST',
            url: '/processor/broadcast_send',
            data: JSON.stringify(data),
            contentType: "application/json",
            dataType: 'json',
            success: function(data) {
                console.log('Success!');
            },
            error: function(err){
                if (err.status !== 200){
                    alert("error");
                }
            }
        });
    });
});