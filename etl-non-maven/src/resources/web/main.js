var reportInterval;
function init(){
	console.log('started v4');
	showReport();
	reportInterval = setInterval(showReport, 5000);
	$('.exit').click(exit);
	$('.set-n').click(setN);
	$('.refresh').click(showReport);
	$('.ins-job').click(insertJob);
	var insjob = $('.insert-job');
	$('.ok', insjob).click(()=>insjob.data('onOk')());
	$('.cancel', insjob).click(()=>insjob.data('onCancel')());
	 $('.insert-job').hide();
}
var data;

async function showReport() {
	var txt = await $.get('../report').promise();
	data=JSON.parse(txt);
	var jobdescs = data.workers.map(x=>x.jobDescription);
	console.log(jobdescs);
	$('.job-desc').remove();
	for(var i=0; i<jobdescs.length; i++) {
		let d = jobdescs[i];
		$('<div/>').addClass('job-desc').text(i+' - '+JSON.stringify(d)).appendTo('.workers');
	}

	$('.db').empty();
	var nitems = "#job="+ data.job[0].count
		+ ", #done="+data.done[0].count
		+ ", #error"+ data.error[0].count;
	
	$('<div class="n-items"/>').text(nitems).appendTo('.db');

	$('<div class="operations"/>').appendTo('.db');
	data.operation
		.map(x=>x.operation+": "+x.count)
		.map(txt=>$('<div/>').text(txt).appendTo('.operations'));
		
	$('<div class="status"/>').appendTo('.db');
	data.status
		.map(x=>x.queue+':'+x.status+": "+x.count)
		.map(txt=>$('<div/>').text(txt).appendTo('.status'));
		
	$('.connections').empty();
	data.connections.map(txt=>$('<div/>').text(txt).appendTo('.connections'))
}

function exit() {
	clearInterval(reportInterval);
	$.get('exit').then(()=>alert("done!"));
	alert("comando inviato");
}

function setN() {
	var n = prompt("quanti workers?");
	if (n===null)
		return;
	n = n-0;
	console.log("setn "+n);
	$.get('setn/'+n).then(()=>console.log("set n="+n+", done!"));
}

function insertJob() {
	var fields = ['queue','priority','operation', 'par1', 'par2', 'par3', 'parentJob', 'extra'];
	var defaults = ['anas-etl','1000','prova', '', '', '', '', ''];

	var panel = $('.insert-job');
	$('.main').hide();
	panel.show();
	var container = $('.form', panel);

	if (!container.data('initialized')) {
		container.data('initialized', true);
		container.empty();
		for(var i=0; i<fields.length; i++) {
			let field = fields[i];
			$(`<div class="label">${field}</div>`).appendTo(container);
			$(`<div class="finput ${field}"><input type="text" value="${defaults[i]}">`).appendTo(container);
		}
		panel.data('onOk', async function() {
			var params = []
			for(var field of fields) {
				var val=encodeURIComponent($(`.finput.${field} input`, container).val());
				params.push(field+'='+val);
			}
			var url = 'insertJob?'+params.join('&');
			alert(url);
			panel.hide();
			$.get(url).promise().then(
				ok=>alert(JSON.stringify(JSON.parse(ok),null,2)), 
				error=>alert(error)
			);
			$('.main').show();
		});
		panel.data('onCancel', function() {
			$('.main').show();
			panel.hide();
		});
	}
}
$(init);