class StylesDfs:

    colors_rgb_cod = {
        'AMARELO': '#FFD700',
        'VERDE': '#228B22',
        'AZUL': '#6495ED',
        'VERMELHO': 'red',
        'LARANJA': 'F4A460',
        'ROSA': '#FF00FF',
        'VERDE AGUA': '#008080',
        'CINZA': '#C0C0C0',
        'BRANCO': 'white'
    }

    order_color_chart_ocorrencias = {
        'CRÍTICA': 1,
        'ELOGIO': 2,
        'INFORMAÇÃO': 3,
        'RECLAMAÇÃO': 4,
        'SOLICITAÇÃO': 5,
        'SUGESTÃO': 6,
        'COMENTÁRIOS E MENÇÕES': 7,
        'OUTRAS INTERAÇÕES': 8
    }

    points_chart_pie = [
        {'fill': {'color': colors_rgb_cod['AMARELO']}, 'border': {'color': colors_rgb_cod['BRANCO'], 'width': 2}},
        {'fill': {'color': colors_rgb_cod['VERDE']}, 'border': {'color': colors_rgb_cod['BRANCO'], 'width': 2}},
        {'fill': {'color': colors_rgb_cod['AZUL']}, 'border': {'color': colors_rgb_cod['BRANCO'], 'width': 2}},
        {'fill': {'color': colors_rgb_cod['VERMELHO']}, 'border': {'color': colors_rgb_cod['BRANCO'], 'width': 2}},
        {'fill': {'color': colors_rgb_cod['LARANJA']}, 'border': {'color': colors_rgb_cod['BRANCO'], 'width': 2}},
        {'fill': {'color': colors_rgb_cod['ROSA']}, 'border': {'color': colors_rgb_cod['BRANCO'], 'width': 2}},
        {'fill': {'color': colors_rgb_cod['VERDE AGUA']}, 'border': {'color': colors_rgb_cod['BRANCO'], 'width': 2}},
        {'fill': {'color': colors_rgb_cod['CINZA']}, 'border': {'color': colors_rgb_cod['BRANCO'], 'width': 2}}
    ]
