let panelNo = 0;


class Panel {
    id_source = null;
    id_stream = null;
    msg_type = null; //str
    msg_type_class = null;
    n = ++panelNo;

    msg_reader = null;
    max_height = 0;

    display_widget = null;
    data_trace = [];
    widget_menu_cb = null;

    max_trace_length = 100;
    zoom = 1;

    grid_widget = null;

    initiated = false;
    resize_event_handler = null;
    src_visible = false;
    //const event = new Event("build");

    //widget_opts = {};

    static TogglePanel(id_source, msg_type, state, w, h, x = null, y = null, src_visible = false, zoom = 1.0) {
        let panel = panels[id_source];
        if (state) {
            if (!panel) {
                panel = new Panel(id_source, w, h, x, y, src_visible, zoom);
                panel.init(msg_type)
            }
        } else if (panel) {
            panel.close();
        }
    }

    constructor(id_source,  w, h, x = null, y = null, src_visible = false, zoom = 1.0) {
        this.id_source = id_source;
        this.src_visible = src_visible;
        this.zoom = zoom;
        console.log('Panel created for '+this.id_source + ' src_visible='+this.src_visible)

        //let display_source = false;

        let html =
            '<div class="grid_panel" data-source="'+id_source+'">' +
                '<h3>'+id_source+'</h3>' +
                '<div class="monitor_menu">' +
                    '<div class="hover_keeper"></div>' +
                    '<div class="monitor_menu_content" id="monitor_menu_content_'+this.n+'"></div>' +
                '</div>' +
                '<div class="panel_content_space">' +
                    '<div class="panel_widget'+(this.src_visible?' source_visible':'')+'" id="panel_widget_'+this.n+'"></div>' +
                    '<div class="panel_source'+(this.src_visible?' enabled':'')+'" id="panel_source_'+this.n+'">Waiting for data...</div>' +
                    '<div class="cleaner"></div>' +
                '</div>' +
                //'<div class="panel_msg_type" id="panel_msg_type_'+this.n+'"></div>' +
            '</div>'

        let widget_opts = {w: w, h:h, content: html};
        if (x != null && x != undefined) widget_opts.x = x;
        if (y != null && y != undefined) widget_opts.y = y;
        panels[id_source] = this;
        this.grid_widget = grid.addWidget(widget_opts);
    }

    // init with message type when it's known
    // might get called with null gefore we receive the message type
    init(msg_type=null) {

        this.msg_type = msg_type;
        if (this.msg_type && this.msg_type != 'video') {

            this.msg_type_class = msg_type ? FindMessageType(this.msg_type, supported_msg_types) : null;
            $('#panel_msg_types_'+this.n).html(this.msg_type ? this.msg_type : '');

            if (this.msg_type_class == null && this.msg_type != null) {
                $('#panel_msg_types_'+this.n).addClass('err');
                $('#panel_source_'+this.n).html('<span class="error">Message type '+ this.msg_type +' not loaded</span>');
            }

            if (this.msg_type != null) {
                let Reader = window.Serialization.MessageReader;
                this.msg_reader = new Reader( [ this.msg_type_class ].concat(supported_msg_types) );
            }

            if (panel_widgets[this.msg_type] != undefined) {
                if (!this.display_widget) { //only once
                    // $('#display_panel_source_link_'+this.n).css('display', 'block');
                    panel_widgets[this.msg_type].widget(this, null) //no data yet
                }
            } else {  //no widget, show source
                //console.error('no widget for '+this.id_source+" msg_type="+this.msg_type)
                $('#panel_source_'+this.n).addClass('enabled');
            }
        }

        this.setMenu()
        this.onResize();
    }

    setMenu() {

        let els = []
        if (this.msg_type != null && this.msg_type != 'video') {
            els.push('<div class="menu_line panel_msg_types_line"><a href="#" id="panel_msg_types_'+this.n+'" class="msg_types" title="Toggle message type definition">'+this.msg_type+'</a></div>');
            els.push('<div class="menu_line"><label for="update_panel_'+this.n+'" class="update_panel_label" id="update_panel_label_'+this.n+'"><input type="checkbox" id="update_panel_'+this.n+'" class="panel_update" checked title="Update"/> Update panel</label></div>');

            if (this.display_widget)
                els.push('<div class="menu_line" id="display_panel_source_link_'+this.n+'"><label for="display_panel_source_'+this.n+'" class="display_panel_source_label" id="display_panel_source_label_'+this.n+'"><input type="checkbox" id="display_panel_source_'+this.n+'" class="panel_display_source"'+(this.src_visible?' checked':'')+' title="Display source data"> Show source data</label></div>');
        }

        els.push('<div class="menu_line"><a href="#" id="close_panel_link_'+this.n+'">Close</a></div>')

        $('#monitor_menu_content_'+this.n).html(els.join('\n'));

        let that = this;
        $('#panel_msg_types_'+this.n).click(function(ev) {

            $('#msg_type-dialog').attr('title', that.msg_type);
            $('#msg_type-dialog').html((that.msg_type_class ? JSON.stringify(that.msg_type_class, null, 2) : '<span class="error">Message type not loaded!</span>'));
            $( "#msg_type-dialog" ).dialog({
                resizable: true,
                height: 700,
                width: 500,
                modal: true,
                buttons: {
                    Okay: function() {
                        $(this).dialog( "close" );
                    },
                }
            });

            ev.cancelBubble = true;
            ev.preventDefault();
        });

        let source_el = $('#panel_source_'+this.n);
        let widget_el = $('#panel_widget_'+this.n);
        $('#display_panel_source_'+this.n).change(function(ev) {
            if ($(this).prop('checked')) {
                source_el.addClass('enabled');
                widget_el.addClass('source_visible');
                that.src_visible = true;

                let w = parseInt($(that.grid_widget).attr('gs-w'));
                if (w < 5) {
                    w *= 2;
                    grid.update(that.grid_widget, {w : w}); //updates url hash, triggers onResize
                } else {
                    that.onResize();
                    Panel.UpdateUrlHash();
                }
            } else {
                source_el.removeClass('enabled');
                widget_el.removeClass('source_visible');
                let w = Math.floor(parseInt($(that.grid_widget).attr('gs-w'))/2);
                that.src_visible = false;
                grid.update(that.grid_widget, {w : w});  //updates url hash, triggers onResize
            }
        });

        $('#close_panel_link_'+this.n).click(function(ev) {
            /*console.log('click '+that.n)
            let el = $('#panel_msg_type_'+that.n);
            if (el.css('display') != 'block')
                el.css('display', 'block');
            else if (!el.hasClass('err'))
                el.css('display', 'none');
                */

            that.close();

            //that.Close();
            //delete panels[that.topic];

            ev.cancelBubble = true;
            ev.preventDefault();
        });

        if (this.widget_menu_cb != null) {
            this.widget_menu_cb(this);
        }

    }

    getAvailableWidgetSize() {

        let w = $('#panel_widget_'+this.n).width();
        let h = $('#panel_widget_'+this.n).parent().innerHeight()-10; //padding

        //let sourceDisplayed = $('#panel_content_'+this.n).hasClass('enabled');

        //console.log('w', w, $('#panel_content_'+panel.n).innerWidth())
        //if (sourceDisplayed)
        //    w -= $('#panel_content_'+panel.n).innerWidth();

        //h = Math.min(h, w);
        //h = Math.min(h, 500);

        return [w, h];
    }

    onResize() {

        [ this.widget_width, this.widget_height ] = this.getAvailableWidgetSize();

        // console.warn('Resizing panel widget for '+ this.id_source+' to '+this.widget_width +' x '+this.widget_height);

        let canvas = document.getElementById('panel_canvas_'+this.n)
        if (canvas) {
            canvas.width = Math.round(this.widget_width);
            canvas.height = Math.round(this.widget_height);
        }

        //  auto scale canvas
        // if ($('#panel_widget_'+this.n+' CANVAS').length > 0) {

        //     $('#panel_widget_'+this.n+' CANVAS')
        //         .attr({
        //             'width': this.widget_width,
        //             'height' : this.widget_height
        //         });
        // }

        // auto scale THREE renderer & set camera aspect
        if (this.renderer) {

            this.camera.aspect = parseFloat(this.widget_width) / parseFloat(this.widget_height);
            this.camera.updateProjectionMatrix();

            this.renderer.setSize( this.widget_width, this.widget_height );
            // console.log('resize', this.widget_width, this.widget_height)
        }

        // let h = $('#panel_content_'+this.n).parent().parent('.grid-stack-item-content').innerHeight();
        // let t = $('#panel_content_'+this.n).position().top;
        // let pt = parseInt($('#panel_content_'+this.n).css('padding-top'));
        // let pb = parseInt($('#panel_content_'+this.n).css('padding-bottom'));
        // let mt = parseInt($('#panel_content_'+this.n).css('margin-top'));
        // let mb = parseInt($('#panel_content_'+this.n).css('margin-bottom'));
        // console.log('resize ', h, t, pt, pb, mt, mb)
        //$('#panel_content_'+this.n).css('height', h-t-pt-pb-mt-mb);

       if (this.resize_event_handler != null)
           this.resize_event_handler();
    }

    onData(ev) {

        let rawData = ev.data; //arraybuffer
        let decoded = null;

        //let oldh = $('#panel_content_'+this.n).height();
        //$('#panel_content_'+this.n).height('auto');
        if (rawData instanceof ArrayBuffer) {

            if (this.id_source == '/robot_description')
                console.warn(this.id_source+' onData (buff): ' + rawData.byteLength + 'B');

            let datahr = '';
            if (this.msg_reader != null) {
                let v = new DataView(rawData)
                decoded = this.msg_reader.readMessage(v);
                if (this.msg_type == 'std_msgs/msg/String' && decoded.data) {
                    if (decoded.data.indexOf('xml') !== -1)  {
                        datahr = linkifyURLs(escapeHtml(window.xmlFormatter(decoded.data)), true);
                    } else {
                        datahr = linkifyURLs(escapeHtml(decoded.data));
                    }
                    //console.log(window.xmlFormatter)

                } else {
                    datahr = JSON.stringify(decoded, null, 2);
                }
                //datahr = rawData.
            } else {
                datahr = buf2hex(rawData)
            }

            $('#panel_source_'+this.n).html(
                'Stamp: '+ev.timeStamp + '<br>' +
                rawData+' '+rawData.byteLength+'B'+'<br>' +
                '<br>' +
                datahr
            );

            if (panel_widgets[this.msg_type] && panel_widgets[this.msg_type].widget)
                panel_widgets[this.msg_type].widget(this, decoded);

        } else { //string data

            let datahr = ev.data;
            if (this.id_source == '/robot_description')
                console.warn(this.id_source+' onData (hr): ', datahr);

            $('#panel_source_'+this.n).html(
                'Stamp: '+ev.timeStamp + '<br>' +
                '<br>' +
                datahr
            );

        }

        let newh = $('#panel_source_'+this.n).height();
        //console.log('max_height='+this.max_height+' newh='+newh);

        if (newh > this.max_height) {
            this.max_height = newh;
        }
        //$('#panel_content_'+this.n).height(this.max_height);


    }

    close() {

        if ($('.topic[data-topic="'+this.id_source+'"] INPUT:checkbox').length > 0) {
            // $('.topic[data-toppic="'+that.id_source+'"] INPUT:checkbox').click();
            $('.topic[data-topic="'+this.id_source+'"] INPUT:checkbox').removeClass('enabled'); //prevent eventhandler
            $('.topic[data-topic="'+this.id_source+'"] INPUT:checkbox').prop('checked', false);
            $('.topic[data-topic="'+this.id_source+'"] INPUT:checkbox').addClass('enabled');

            SetTopicsReadSubscription(id_robot, [ this.id_source ], false);
        } // else { //topics not loaded
            // Panel.TogglePanel(that.id_source, null, false);
        // }

        // let x = parseInt($(this.grid_widget).attr('gs-x'));
        // let y = parseInt($(this.grid_widget).attr('gs-y'));

        grid.removeWidget(this.grid_widget);

        delete panels[this.id_source];

        $('.grid_panel[data-source="'+this.id_source+'"]').remove(); //updates url hash
        console.log('Panel closed for '+this.id_source)
    }

    static UpdateUrlHash() {
        let hash = [];

        //console.log('Hash for :', $('#grid-stack').children('.grid-stack-item'));

        $('#grid-stack').children('.grid-stack-item').each(function () {
            let widget = this;
            let x = $(widget).attr('gs-x');
            let y = $(widget).attr('gs-y');
            let w = $(widget).attr('gs-w');
            let h = $(widget).attr('gs-h');
            let id_source = $(widget).find('.grid_panel').attr('data-source');

            let parts = [
                id_source,
                [x, y].join('x'),
                [w, h].join('x'),
            ];
            if (panels[id_source].src_visible)
                parts.push('src');
            if (panels[id_source].zoom != 1)
                parts.push('z='+panels[id_source].zoom);

            hash.push(parts.join(':'));
        });

        if (hash.length > 0)
            window.location.hash = ''+hash.join(';');
        else //remove hash
            history.pushState("", document.title, window.location.pathname+window.location.search);
    }

    static ParseUrlHash(hash) {
        if (!hash.length) {
            return
        }

        hash = hash.substr(1);
        let hashArr = hash.split(';');
        for (let i = 0; i < hashArr.length; i++) {
            let src_vars = hashArr[i].trim();
            if (!src_vars)
                continue;
            src_vars = src_vars.split(':');
            let id_source = src_vars[0];
            let x = null; let y = null;
            let panelPos = src_vars[1];
            if (panelPos) {
                panelPos = panelPos.split('x');
                x = panelPos[0];
                y = panelPos[1];
            }
            let panelSize = src_vars[2];
            let w = 2; let h = 2;
            if (panelSize) {
                panelSize = panelSize.split('x');
                w = panelSize[0];
                h = panelSize[1];
            }

            //opional vars follow
            let src_on = false;
            let zoom = 1;
            for (let j = 2; j < src_vars.length; j++) {
                if (src_vars[j] == 'src') {
                    src_on = true;
                }
                else if (src_vars[j].indexOf('z=') === 0) {
                    zoom = parseFloat(src_vars[j].substr(2));
                    console.log('Found zoom for '+id_source+': '+src_vars[j] + ' => ', zoom);
                }
            }

            let msg_type = null; //unknown atm
            //console.info('Opening panel for '+topic+'; src_on='+src_on);
            Panel.TogglePanel(id_source, msg_type, true, w, h, x, y, src_on, zoom)
        }

    }

}