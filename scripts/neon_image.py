import io
import cairosvg
from PIL import Image, ImageDraw, ImageFilter

# Create a new image with a gradient background
def create_neon_icon(size=512):
    # Create a new image with RGBA mode
    image = Image.new('RGBA', (size, size), (255, 255, 255, 0))
    
    # Create a drawing context
    draw = ImageDraw.Draw(image)
    
    # Create gradient background
    for y in range(size):
        # Interpolate between blue and purple
        r = int(74 + (121 - 74) * y / size)
        g = int(144 + (21 - 144) * y / size)
        b = int(226 + (247 - 226) * y / size)
        for x in range(size):
            draw.point((x, y), fill=(r, g, b))
    
    # Draw circle
    circle_radius = int(size * 0.8 / 2)
    circle_center = (size // 2, size // 2)
    draw.ellipse([
        circle_center[0] - circle_radius, 
        circle_center[1] - circle_radius,
        circle_center[0] + circle_radius, 
        circle_center[1] + circle_radius
    ], fill=(255, 255, 255, 50))
    
    # Draw stylized circuit lines
    line_color = (255, 255, 255, 180)
    line_width = size // 50
    
    # Top circuit line
    draw.line([
        (size * 0.2, size * 0.4), 
        (size * 0.5, size * 0.3), 
        (size * 0.8, size * 0.4)
    ], fill=line_color, width=line_width)
    
    # Middle circuit line
    draw.line([
        (size * 0.15, size * 0.5), 
        (size * 0.5, size * 0.45), 
        (size * 0.85, size * 0.5)
    ], fill=line_color, width=line_width)
    
    # Bottom circuit line
    draw.line([
        (size * 0.2, size * 0.6), 
        (size * 0.5, size * 0.7), 
        (size * 0.8, size * 0.6)
    ], fill=line_color, width=line_width)
    
    # Draw stylized N
    n_color = (255, 255, 255, 220)
    n_width = size // 25
    
    # Left vertical line of N
    draw.line([
        (size * 0.35, size * 0.3), 
        (size * 0.35, size * 0.7)
    ], fill=n_color, width=n_width)
    
    # Diagonal line of N
    draw.line([
        (size * 0.35, size * 0.3), 
        (size * 0.65, size * 0.7)
    ], fill=n_color, width=n_width)
    
    # Right vertical line of N
    draw.line([
        (size * 0.65, size * 0.3), 
        (size * 0.65, size * 0.7)
    ], fill=n_color, width=n_width)
    
    # Add a soft glow effect
    image = image.filter(ImageFilter.GaussianBlur(radius=size//100))
    
    return image

# Generate and save the icon
icon = create_neon_icon(512)
icon.save('neon_icon.png')
print("Neon icon saved as neon_icon.png")