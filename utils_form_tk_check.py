import ipywidgets as widgets
from ipywidgets import Layout, Box, Label

def form_param_esklp_exist_dicts(esklp_dates):
    esklp_dates_dropdown = widgets.Dropdown( options=esklp_dates) #, value=None)
    
    form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')
    check_box = Box([Label(value="Выберите дату сохраненного справочника ЕСКЛП:"), esklp_dates_dropdown], layout=form_item_layout) 
    form_items = [check_box]
    
    form_esklp_exist_dicts = Box(form_items, layout=Layout(display='flex', flex_flow= 'column', border='solid 2px', align_items='stretch', width='50%')) #width='auto'))
    # return form, fn_check_file_drop_douwn, fn_dict_file_drop_douwn, radio_btn_big_dict, radio_btn_prod_options, similarity_threshold_slider, max_entries_slider
    return form_esklp_exist_dicts, esklp_dates_dropdown 
