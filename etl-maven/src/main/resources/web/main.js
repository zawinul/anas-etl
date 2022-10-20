function init(){
	console.log('started v4');
	showReport();
	setInterval(showReport, 2000);
}
var data;
async function showReport() {
	var txt = await $.get('../report').promise();
	data=JSON.parse(txt);
	var jobdescs = data.workers.map(x=>x.jobDescription);
	console.log(jobdescs);
	$('.job-desc').remove();
	for(var d of jobdescs)
		$('<div/>').addClass('job-desc').text(d).appendTo('body');
}
$(init);